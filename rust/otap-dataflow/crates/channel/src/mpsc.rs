// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Multiple-producer, single-consumer channel implementation optimized for single-threaded async.

use crate::error::{RecvError, SendError};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    /// Total number of times SendFuture::poll was called.
    pub(crate) static SEND_POLL_TOTAL: Cell<usize> = const { Cell::new(0) };
    /// Number of times SendFuture::poll found the buffer full (wasted poll).
    pub(crate) static SEND_POLL_FULL: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SenderWaiterKey {
    // Stable key into the waiter slot array; generation prevents ABA when a
    // slot index is reused after cancellation/completion.
    index: usize,
    generation: u64,
}

struct SenderWaiterSlot {
    generation: u64,
    waker: Option<Waker>,
    in_use: bool,
    queued: bool,
    sender_id: usize,
}

impl SenderWaiterSlot {
    const fn vacant() -> Self {
        Self {
            generation: 0,
            waker: None,
            in_use: false,
            queued: false,
            sender_id: 0,
        }
    }
}

struct SenderWaiters {
    // FIFO queue of waiter keys used to preserve wake order across producers.
    queue: VecDeque<SenderWaiterKey>,
    // Slot storage allows O(1) refresh/unregister by key without scanning the queue.
    slots: Vec<SenderWaiterSlot>,
    // Reuse released slots to avoid per-contention allocations.
    free_slots: Vec<usize>,
    next_generation: u64,
}

impl SenderWaiters {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            slots: Vec::new(),
            free_slots: Vec::new(),
            next_generation: 0,
        }
    }

    fn wake_one(&mut self, skip_sender_id: Option<usize>) {
        let mut skipped_key: Option<SenderWaiterKey> = None;
        while let Some(key) = self.queue.pop_front() {
            let Some(slot) = self.slots.get_mut(key.index) else {
                continue;
            };
            // Stale queue entries are expected when futures are canceled or
            // re-queued; skip until we find a live queued waiter.
            if !slot.in_use || slot.generation != key.generation || !slot.queued {
                continue;
            }
            // If this waiter belongs to the sender we want to deprioritize,
            // push it to the back and try the next one. Only skip once --
            // if all waiters belong to the same sender, wake it anyway.
            if skipped_key.is_none()
                && skip_sender_id == Some(slot.sender_id)
                && !self.queue.is_empty()
            {
                skipped_key = Some(key);
                self.queue.push_back(key);
                continue;
            }
            slot.queued = false;
            if let Some(waker) = slot.waker.take() {
                waker.wake();
                return;
            }
        }
    }

    fn wake_all(&mut self) {
        while let Some(key) = self.queue.pop_front() {
            let Some(slot) = self.slots.get_mut(key.index) else {
                continue;
            };
            // Same stale-entry filtering as wake_one, but drain everything.
            if !slot.in_use || slot.generation != key.generation || !slot.queued {
                continue;
            }
            slot.queued = false;
            if let Some(waker) = slot.waker.take() {
                waker.wake();
            }
        }
    }

    fn register_or_refresh(
        &mut self,
        waiter_key: &mut Option<SenderWaiterKey>,
        waker: &Waker,
        sender_id: usize,
    ) {
        if let Some(existing_key) = *waiter_key
            && let Some(slot) = self.slots.get_mut(existing_key.index)
            && slot.in_use
            && slot.generation == existing_key.generation
        {
            if slot.waker.as_ref().is_none_or(|w| !w.will_wake(waker)) {
                slot.waker = Some(waker.clone());
            }
            if !slot.queued {
                slot.queued = true;
                self.queue.push_back(existing_key);
            }
            return;
        }

        let index = if let Some(index) = self.free_slots.pop() {
            index
        } else {
            self.slots.push(SenderWaiterSlot::vacant());
            self.slots.len() - 1
        };

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);

        let slot = &mut self.slots[index];
        slot.generation = generation;
        slot.waker = Some(waker.clone());
        slot.in_use = true;
        slot.queued = true;
        slot.sender_id = sender_id;

        let key = SenderWaiterKey { index, generation };
        self.queue.push_back(key);
        *waiter_key = Some(key);
    }

    fn unregister(&mut self, waiter_key: SenderWaiterKey) {
        let Some(slot) = self.slots.get_mut(waiter_key.index) else {
            return;
        };
        if !slot.in_use || slot.generation != waiter_key.generation {
            return;
        }
        slot.in_use = false;
        slot.queued = false;
        slot.waker = None;
        self.free_slots.push(waiter_key.index);
    }
}

struct ChannelState<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    is_closed: bool,
    senders: usize,
    has_receiver: bool,
    receiver_waker: Option<Waker>,
    // Waiters are allocated only when the channel becomes full, so uncontended
    // send/recv paths keep the old small state footprint.
    sender_waiters: Option<SenderWaiters>,
    // Tracks the sender that most recently pushed an item, so the receiver
    // can deprioritize it when waking blocked senders. This improves fairness
    // by giving other senders a chance before the last active sender runs
    // again.
    last_sender_id: Option<usize>,
    next_sender_id: usize,
}

impl<T> ChannelState<T> {
    fn wake_one_sender_waiter(&mut self) {
        if let Some(waiters) = self.sender_waiters.as_mut() {
            waiters.wake_one(self.last_sender_id);
        }
    }

    fn wake_all_sender_waiters(&mut self) {
        if let Some(waiters) = self.sender_waiters.as_mut() {
            waiters.wake_all();
        }
    }

    fn register_or_refresh_sender_waiter(
        &mut self,
        waiter_key: &mut Option<SenderWaiterKey>,
        waker: &Waker,
        sender_id: usize,
    ) {
        // Keyed waiter slots fix canceled-send stale wakers without wake-all.
        self.sender_waiters
            .get_or_insert_with(SenderWaiters::new)
            .register_or_refresh(waiter_key, waker, sender_id);
    }

    fn unregister_sender_waiter(&mut self, waiter_key: SenderWaiterKey) {
        if let Some(waiters) = self.sender_waiters.as_mut() {
            waiters.unregister(waiter_key);
        }
    }
}

/// A single-threaded MPSC channel.
pub struct Channel<T> {
    state: RefCell<ChannelState<T>>,
}

impl<T> Channel<T> {
    /// Creates a new channel with the given capacity.
    #[allow(clippy::new_ret_no_self)]
    #[must_use]
    pub fn new(capacity: usize) -> (Sender<T>, Receiver<T>) {
        let channel = Rc::new(Channel {
            state: RefCell::new(ChannelState {
                buffer: VecDeque::with_capacity(capacity),
                capacity,
                is_closed: false,
                senders: 1,
                has_receiver: true,
                receiver_waker: None,
                sender_waiters: None,
                last_sender_id: None,
                next_sender_id: 1,
            }),
        });

        (
            Sender {
                channel: channel.clone(),
                sender_id: 0,
            },
            Receiver { channel },
        )
    }
}

/// A sender for the channel.
pub struct Sender<T> {
    channel: Rc<Channel<T>>,
    sender_id: usize,
}

/// A receiver for the channel.
pub struct Receiver<T> {
    channel: Rc<Channel<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let sender_id;
        {
            let mut state = self.channel.state.borrow_mut();
            state.senders += 1;
            sender_id = state.next_sender_id;
            state.next_sender_id += 1;
        }
        Sender {
            channel: self.channel.clone(),
            sender_id,
        }
    }
}

impl<T> Sender<T> {
    /// Clone that preserves the sender_id (for internal use by SendFuture).
    fn clone_internal(&self) -> Self {
        let mut state = self.channel.state.borrow_mut();
        state.senders += 1;
        Sender {
            channel: self.channel.clone(),
            sender_id: self.sender_id,
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        state.senders -= 1;

        if state.senders == 0 {
            state.is_closed = true;
            if let Some(waker) = state.receiver_waker.take() {
                waker.wake();
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        state.has_receiver = false;
        state.is_closed = true;
        state.wake_all_sender_waiters();
    }
}

impl<T> Sender<T> {
    /// Sends a value to the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut state = self.channel.state.borrow_mut();

        if state.is_closed || !state.has_receiver {
            return Err(SendError::Closed(value));
        }

        if state.buffer.len() >= state.capacity {
            return Err(SendError::Full(value));
        }

        state.buffer.push_back(value);
        state.last_sender_id = Some(self.sender_id);

        if let Some(waker) = state.receiver_waker.take() {
            waker.wake();
        }

        Ok(())
    }

    /// Sends a value to the channel asynchronously.
    pub async fn send_async(&self, value: T) -> Result<(), SendError<T>> {
        // Fast path: avoid creating/polling a future when there is capacity.
        match self.send(value) {
            Ok(()) => Ok(()),
            Err(SendError::Full(value)) => {
                SendFuture {
                    sender: self.clone_internal(),
                    value: Some(value),
                    waiter_key: None,
                }
                .await
            }
            Err(error) => Err(error),
        }
    }

    /// Closes the channel.
    pub fn close(&self) {
        let mut state = self.channel.state.borrow_mut();
        state.is_closed = true;
        // Wake the receiver if it's waiting
        if let Some(waker) = state.receiver_waker.take() {
            waker.wake();
        }
        state.wake_all_sender_waiters();
    }
}

impl<T> Receiver<T> {
    /// Tries to receive a value from the channel.
    pub fn try_recv(&self) -> Result<T, RecvError> {
        let mut state = self.channel.state.borrow_mut();

        if let Some(value) = state.buffer.pop_front() {
            state.wake_one_sender_waiter();
            Ok(value)
        } else if state.is_closed {
            Err(RecvError::Closed)
        } else {
            Err(RecvError::Empty)
        }
    }

    /// Receives a value from the channel asynchronously.
    pub async fn recv(&self) -> Result<T, RecvError> {
        RecvFuture { receiver: self }.await
    }

    /// Checks if the channel is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let state = self.channel.state.borrow();
        state.buffer.is_empty()
    }

    /// Checks whether the channel has been closed and will accept no further
    /// sends.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        let state = self.channel.state.borrow();
        state.is_closed
    }
}

struct SendFuture<T> {
    sender: Sender<T>,
    value: Option<T>,
    waiter_key: Option<SenderWaiterKey>,
}

impl<T> Unpin for SendFuture<T> {}

impl<T> Drop for SendFuture<T> {
    fn drop(&mut self) {
        let Some(waiter_key) = self.waiter_key.take() else {
            return;
        };
        // `tokio::select!` cancellation is common: remove blocked sender waiter.
        let mut state = self.sender.channel.state.borrow_mut();
        state.unregister_sender_waiter(waiter_key);
    }
}

impl<T> Future for SendFuture<T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(test)]
        SEND_POLL_TOTAL.with(|c| c.set(c.get() + 1));

        let value = self
            .value
            .take()
            .expect("SendFuture polled after completion");

        match self.sender.send(value) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(SendError::Full(value)) => {
                #[cfg(test)]
                SEND_POLL_FULL.with(|c| c.set(c.get() + 1));

                self.value = Some(value);
                // Persist the waiter key across polls so we can refresh in place.
                let mut waiter_key = self.waiter_key;
                {
                    let mut state = self.sender.channel.state.borrow_mut();
                    state.register_or_refresh_sender_waiter(
                        &mut waiter_key,
                        cx.waker(),
                        self.sender.sender_id,
                    );
                }
                self.waiter_key = waiter_key;
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

struct RecvFuture<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        match self.receiver.try_recv() {
            Ok(value) => Poll::Ready(Ok(value)),
            Err(RecvError::Empty) => {
                let mut state = self.receiver.channel.state.borrow_mut();
                if state
                    .receiver_waker
                    .as_ref()
                    .is_none_or(|w| !w.will_wake(cx.waker()))
                {
                    state.receiver_waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::time::{Duration, timeout};

    // Helper function to create a test runtime
    fn create_test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn test_basic_channel_operations() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(2);

            // Test send and receive
            let result = tx.send(1);
            assert!(result.is_ok());

            let result = tx.send(2);
            assert!(result.is_ok());
            assert_eq!(rx.try_recv().unwrap(), 1);
            assert_eq!(rx.try_recv().unwrap(), 2);

            // Test empty channel
            assert!(matches!(rx.try_recv(), Err(RecvError::Empty)));
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_channel_capacity() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, _rx) = Channel::new(1);

            // First send should succeed
            let result = tx.send(1);
            assert!(result.is_ok());

            // Second send should fail with Full error
            match tx.send(2) {
                Err(SendError::Full(2)) => (),
                _ => panic!("Expected Full error"),
            }
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_multiple_producers() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx1, rx) = Channel::new(4);
            let tx2 = tx1.clone();

            // Send from both producers
            let result = tx1.send(1);
            assert!(result.is_ok());
            let result = tx2.send(2);
            assert!(result.is_ok());

            // Verify order
            assert_eq!(rx.try_recv().unwrap(), 1);
            assert_eq!(rx.try_recv().unwrap(), 2);
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_async_send_receive() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(1);
            let received = Rc::new(RefCell::new(vec![]));
            let received_clone = received.clone();

            // Spawn consumer
            let consumer = tokio::task::spawn_local(async move {
                while let Ok(value) = rx.recv().await {
                    received_clone.borrow_mut().push(value);
                    if value == 2 {
                        break;
                    }
                }
            });

            // Send values
            let result = tx.send_async(1).await;
            assert!(result.is_ok());
            let result = tx.send_async(2).await;
            assert!(result.is_ok());

            consumer.await.unwrap();
            assert_eq!(*received.borrow(), vec![1, 2]);
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_channel_closing() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(1);

            // Send a value
            let result = tx.send(1);
            assert!(result.is_ok());

            // Close the channel
            tx.close();

            // Should still be able to receive existing value
            assert_eq!(rx.try_recv().unwrap(), 1);

            // Further receives should indicate closed channel
            assert!(matches!(rx.try_recv(), Err(RecvError::Closed)));

            // Sends should fail with Closed error
            match tx.send(2) {
                Err(SendError::Closed(2)) => (),
                _ => panic!("Expected Closed error"),
            }
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_sender_drop() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(1);

            let result = tx.send(1);
            assert!(result.is_ok());
            drop(tx);

            // Should still receive last value
            assert_eq!(rx.recv().await.unwrap(), 1);

            // Next receive should indicate closed
            assert!(matches!(rx.recv().await, Err(RecvError::Closed)));
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_backpressure() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(1);
            let send_completed = Rc::new(RefCell::new(false));
            let send_completed_clone = send_completed.clone();

            // Spawn producer task that will block
            let producer = tokio::task::spawn_local(async move {
                let result = tx.send(1);
                assert!(result.is_ok());

                let result = tx.send_async(2).await;
                assert!(result.is_ok());
                *send_completed_clone.borrow_mut() = true;
            });

            // Allow some time for the second send to block
            tokio::task::yield_now().await;
            assert!(!*send_completed.borrow());

            // Receive value, allowing blocked send to complete
            assert_eq!(rx.recv().await.unwrap(), 1);

            // Wait for producer to complete
            timeout(Duration::from_millis(100), producer)
                .await
                .expect("producer should complete")
                .unwrap();

            assert!(*send_completed.borrow());
            assert_eq!(rx.recv().await.unwrap(), 2);
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_fairness_in_waking_senders() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(1);
            let received = Rc::new(RefCell::new(vec![]));
            let received_clone = received.clone();

            // Send a value to fill the channel to its capacity
            let result = tx.send_async(1).await;
            assert!(result.is_ok());

            // Spawn senders that wait for the reciver to be process items and wake them up
            let sender_clone1 = tx.clone();
            let sender_clone2 = tx.clone();

            let pending_sender_1 = tokio::task::spawn_local(async move {
                let result = sender_clone1.send_async(2).await;
                assert!(result.is_ok());
            });

            let pending_sender_2 = tokio::task::spawn_local(async move {
                let result = sender_clone2.send_async(3).await;
                assert!(result.is_ok());
            });

            // Spawn consumer
            let consumer = tokio::task::spawn_local(async move {
                let mut count_of_items_processed = 0;
                const MAX_ITEMS_TO_RECEIVE: usize = 3;
                while let Ok(value) = rx.recv().await {
                    received_clone.borrow_mut().push(value);
                    count_of_items_processed += 1;
                    if count_of_items_processed >= MAX_ITEMS_TO_RECEIVE {
                        break;
                    }
                }
            });

            pending_sender_1.await.unwrap();
            pending_sender_2.await.unwrap();
            consumer.await.unwrap();
            assert_eq!(*received.borrow(), vec![1, 2, 3]); // Wake the sender in FIFO order. We should receive 1 -> 2 -> 3 and not 1 -> 3 -> 2
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    #[test]
    fn test_canceled_send_does_not_block_waiting_sender() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let (tx, rx) = Channel::new(1);
            tx.send(0).expect("initial send must succeed");

            // Register a sender waker, then cancel the future to leave a stale
            // waiter in the queue.
            let tx_canceled = tx.clone();
            let canceled = tokio::task::spawn_local(async move {
                tokio::select! {
                    _ = tx_canceled.send_async(1) => panic!("send should stay blocked while channel is full"),
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            });
            canceled.await.expect("canceled task should complete");

            // A real waiting sender should still make progress after one recv.
            let tx_waiting = tx.clone();
            let waiting = tokio::task::spawn_local(async move {
                tx_waiting
                    .send_async(2)
                    .await
                    .expect("waiting sender should be awakened");
            });

            tokio::task::yield_now().await;
            assert_eq!(rx.recv().await.expect("must receive first value"), 0);

            timeout(Duration::from_millis(200), waiting)
                .await
                .expect("waiting sender timed out")
                .expect("waiting sender task failed");

            assert_eq!(rx.recv().await.expect("must receive second value"), 2);
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    async fn run_diagnostic(num_producers: usize, capacity: usize, total_messages: usize) {
        let per_producer = total_messages / num_producers;

        SEND_POLL_TOTAL.with(|c| c.set(0));
        SEND_POLL_FULL.with(|c| c.set(0));

        let (tx, rx) = Channel::new(capacity);

        let mut producers = Vec::with_capacity(num_producers);
        for p in 0..num_producers {
            let tx_clone = tx.clone();
            producers.push(tokio::task::spawn_local(async move {
                for _ in 0..per_producer {
                    tx_clone
                        .send_async(p)
                        .await
                        .expect("send must succeed");
                }
            }));
        }
        drop(tx);

        let consumer = tokio::task::spawn_local(async move {
            let mut count = 0usize;
            while let Ok(_v) = rx.recv().await {
                count += 1;
            }
            count
        });

        let start = std::time::Instant::now();

        let result = timeout(Duration::from_secs(60), async {
            for p in producers {
                p.await.expect("producer task must not panic");
            }
            consumer.await.expect("consumer task must not panic")
        }).await;

        let elapsed = start.elapsed();

        let poll_total = SEND_POLL_TOTAL.with(|c| c.get());
        let poll_full = SEND_POLL_FULL.with(|c| c.get());
        let poll_ok = poll_total - poll_full;
        let actual_total = per_producer * num_producers;

        println!(
            "--- {num_producers} senders, capacity {capacity}, \
             {actual_total} msgs ---"
        );
        println!("wall time:              {elapsed:?}");
        println!("SendFuture::poll total: {poll_total}");
        println!("  -> Ok (useful):       {poll_ok}");
        println!("  -> Full (wasted):     {poll_full}");
        println!(
            "  wasted/useful ratio:  {:.2}",
            poll_full as f64 / poll_ok.max(1) as f64
        );

        match result {
            Ok(received) => {
                assert_eq!(received, actual_total);
            }
            Err(_) => {
                panic!(
                    "deadlocked: {num_producers} senders, capacity {capacity}"
                );
            }
        }
    }

    /// Scenario: 8 producers, capacity 4.
    /// Guarantees: measures wall-clock and wasted polls at small capacity.
    #[test]
    fn test_diagnostic_8_senders_cap_4() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();
        let handle = local.spawn_local(async {
            run_diagnostic(8, 4, 4_000_000).await;
        });
        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    /// Scenario: 8 producers, capacity 250.
    /// Guarantees: measures wall-clock and wasted polls at medium capacity.
    #[test]
    fn test_diagnostic_8_senders_cap_250() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();
        let handle = local.spawn_local(async {
            run_diagnostic(8, 250, 4_000_000).await;
        });
        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    /// Scenario: 8 producers, capacity 1000.
    /// Guarantees: measures wall-clock and wasted polls at large capacity.
    #[test]
    fn test_diagnostic_8_senders_cap_1000() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();
        let handle = local.spawn_local(async {
            run_diagnostic(8, 1000, 4_000_000).await;
        });
        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    /// A lightweight yield that returns Pending once with an immediate
    /// wake, without going through tokio's deferred-wake / driver-poll path.
    async fn yield_once() {
        let mut yielded = false;
        core::future::poll_fn(|cx| {
            if yielded {
                return Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
    }

    /// Scenario: 4 producers send to a channel with capacity 64. The
    /// consumer yields (via immediate re-wake, not tokio::yield_now) after
    /// every recv, simulating a slow consumer that does work between reads.
    /// Verifies that: (a) the channel actually fills and senders block
    /// (SEND_POLL_FULL > 0), confirming the wakeup path is exercised, and
    /// (b) all messages are delivered.
    /// Guarantees: the sender-wakeup path is exercised even when the
    /// consumer yields on every recv, and no messages are lost.
    #[test]
    fn test_consumer_yield_every_recv_exercises_wakeup() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let num_producers = 4usize;
            let capacity = 64usize;
            let per_producer = 1_000usize;
            let total_messages = num_producers * per_producer;

            SEND_POLL_TOTAL.with(|c| c.set(0));
            SEND_POLL_FULL.with(|c| c.set(0));

            let (tx, rx) = Channel::new(capacity);

            let mut producers = Vec::with_capacity(num_producers);
            for p in 0..num_producers {
                let tx_clone = tx.clone();
                producers.push(tokio::task::spawn_local(async move {
                    for _ in 0..per_producer {
                        tx_clone
                            .send_async(p)
                            .await
                            .expect("send must succeed");
                    }
                }));
            }
            drop(tx);

            let consumer = tokio::task::spawn_local(async move {
                let mut count = 0usize;
                while let Ok(_v) = rx.recv().await {
                    count += 1;
                    yield_once().await;
                }
                count
            });

            let result = timeout(Duration::from_secs(10), async {
                for p in producers {
                    p.await.expect("producer task must not panic");
                }
                consumer.await.expect("consumer task must not panic")
            })
            .await;

            let poll_total = SEND_POLL_TOTAL.with(|c| c.get());
            let poll_full = SEND_POLL_FULL.with(|c| c.get());

            let received = result.expect("must not deadlock");
            assert_eq!(received, total_messages, "all messages must be delivered");
            println!(
                "poll_total={poll_total}, poll_full={poll_full} \
                 (full/total = {:.1}%)",
                100.0 * poll_full as f64 / poll_total.max(1) as f64
            );
            assert!(
                poll_full > 0,
                "senders must have blocked at least once \
                 (poll_total={poll_total}, poll_full={poll_full}), \
                 confirming the wakeup path was exercised"
            );
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }

    /// Scenario: 8 producers send to a channel with capacity 4 (tight loop,
    /// no yields). Tracks which sender produced each message received by the
    /// consumer to detect whether all senders get fair access or some are
    /// starved.
    /// Guarantees: reveals whether the fix introduces sender starvation in
    /// the never-yield worst case.
    #[test]
    fn test_sender_fairness_tight_loop() {
        let rt = create_test_runtime();
        let local = tokio::task::LocalSet::new();

        let handle = local.spawn_local(async {
            let num_producers = 8usize;
            let capacity = 4usize;
            let per_producer = 1_000usize;
            let total_messages = num_producers * per_producer;

            let (tx, rx) = Channel::new(capacity);

            let mut producers = Vec::with_capacity(num_producers);
            for p in 0..num_producers {
                let tx_clone = tx.clone();
                producers.push(tokio::task::spawn_local(async move {
                    for _ in 0..per_producer {
                        tx_clone
                            .send_async(p)
                            .await
                            .expect("send must succeed");
                    }
                }));
            }
            drop(tx);

            // Record the sender ID of every received message
            let recv_order = Rc::new(RefCell::new(Vec::<usize>::with_capacity(total_messages)));
            let order_clone = recv_order.clone();

            let consumer = tokio::task::spawn_local(async move {
                let mut count = 0usize;
                while let Ok(sender_id) = rx.recv().await {
                    order_clone.borrow_mut().push(sender_id);
                    count += 1;
                }
                count
            });

            let result = timeout(Duration::from_secs(10), async {
                for p in producers {
                    p.await.expect("producer task must not panic");
                }
                consumer.await.expect("consumer task must not panic")
            })
            .await;

            let received = result.expect("must not deadlock");
            assert_eq!(received, total_messages);

            let order = recv_order.borrow();

            // Print the first 40 messages to see the pattern
            println!("first 40 messages (sender IDs): {:?}", &order[..40]);

            // Print per-sender progress at checkpoints
            let mut counts = vec![0usize; num_producers];
            let checkpoint_interval = total_messages / 4;
            for (i, &sender_id) in order.iter().enumerate() {
                counts[sender_id] += 1;
                if (i + 1) % checkpoint_interval == 0 {
                    let pct: Vec<String> = counts
                        .iter()
                        .map(|c| {
                            format!(
                                "{:.0}%",
                                100.0 * *c as f64 / per_producer as f64
                            )
                        })
                        .collect();
                    println!(
                        "checkpoint {}: {} msgs, per-sender progress: [{}]",
                        (i + 1) / checkpoint_interval,
                        i + 1,
                        pct.join(", ")
                    );
                }
            }

            // Verify all senders completed
            for (i, count) in counts.iter().enumerate() {
                assert_eq!(
                    *count, per_producer,
                    "sender {i} sent {count}/{per_producer} messages"
                );
            }
        });

        rt.block_on(local);
        rt.block_on(handle).expect("Test task failed");
    }
}
