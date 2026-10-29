// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmark the local `mpsc::Channel` with multiple concurrent producers.
//!
//! Measures throughput across varying producer counts and channel capacities.
//! Small capacities force frequent sender-wakeup interactions and exercise the
//! backpressure path, while larger capacities show peak throughput when senders
//! rarely block.

#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use otel_arrow_dfe_channel::mpsc;
use std::cell::Cell;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Builder;
use tokio::task::LocalSet;

/// A lightweight yield that returns `Pending` once every `interval` calls,
/// immediately re-waking the task without going through the tokio driver.
/// This simulates a producer doing work between sends without paying the
/// cost of a kevent syscall on each yield.
struct PeriodicYield {
    interval: usize,
    count: Cell<usize>,
    yielded: Cell<bool>,
}

impl PeriodicYield {
    fn new(interval: usize) -> Self {
        Self {
            interval,
            count: Cell::new(0),
            yielded: Cell::new(false),
        }
    }

    /// Call this in the send loop. Returns a future that is immediately
    /// ready most of the time, but returns Pending (with an immediate
    /// wake) every `interval` calls.
    fn maybe_yield(&self) -> MaybeYieldFuture<'_> {
        let c = self.count.get() + 1;
        self.count.set(c);
        let should_yield = c % self.interval == 0;
        self.yielded.set(false);
        MaybeYieldFuture {
            parent: self,
            should_yield,
        }
    }
}

struct MaybeYieldFuture<'a> {
    parent: &'a PeriodicYield,
    should_yield: bool,
}

impl Future for MaybeYieldFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.should_yield || self.parent.yielded.get() {
            return Poll::Ready(());
        }
        self.parent.yielded.set(true);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(not(windows))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// Total messages must be divisible by every (num_producers * capacity)
// combination below so that each sender fills the channel an exact number
// of times. This avoids a partial final cycle that would trigger the
// sender-wakeup bug on unpatched code.
const TOTAL_MESSAGES: usize = 1_048_576; // 1024 * 8 * 128

// (num_producers, channel_capacity)
//
// Capacities are chosen to reflect realistic pipeline configurations
// (256-1024) plus a smaller value (64) that exercises the backpressure
// path more aggressively. Producer counts range from 1 (baseline) up to
// 8 (fan-in from multiple upstream nodes).
const PRODUCERS_AND_CAPACITIES: &[(usize, usize)] = &[
    (1, 1024),
    (2, 1024),
    (4, 1024),
    (8, 1024),
    (1, 256),
    (2, 256),
    (4, 256),
    (8, 256),
    (1, 64),
    (2, 64),
    (4, 64),
    (8, 64),
];

async fn run_workload(
    num_producers: usize,
    capacity: usize,
    producer_yield_every: Option<usize>,
    consumer_yield_every: Option<usize>,
) -> usize {
    let (tx, rx) = mpsc::Channel::new(capacity);

    let per_producer = TOTAL_MESSAGES / num_producers;
    debug_assert_eq!(
        per_producer % capacity,
        0,
        "per_producer ({per_producer}) must be a multiple of capacity ({capacity})"
    );
    debug_assert_eq!(
        TOTAL_MESSAGES % num_producers,
        0,
        "TOTAL_MESSAGES must be divisible by num_producers"
    );
    let mut producers = Vec::with_capacity(num_producers);
    for p in 0..num_producers {
        let tx_clone = tx.clone();
        let offset = p * per_producer;
        producers.push(tokio::task::spawn_local(async move {
            let yielder = producer_yield_every.map(PeriodicYield::new);
            for i in 0..per_producer {
                tx_clone
                    .send_async(offset + i)
                    .await
                    .expect("send must succeed");
                if let Some(y) = &yielder {
                    y.maybe_yield().await;
                }
            }
        }));
    }
    // Drop the original sender so the channel closes when all producers finish.
    drop(tx);

    let consumer = tokio::task::spawn_local(async move {
        let yielder = consumer_yield_every.map(PeriodicYield::new);
        let mut count = 0usize;
        while let Ok(_v) = rx.recv().await {
            count += 1;
            if let Some(y) = &yielder {
                y.maybe_yield().await;
            }
        }
        count
    });

    for p in producers {
        p.await.expect("producer task must not panic");
    }
    let received = consumer.await.expect("consumer task must not panic");
    assert_eq!(received, per_producer * num_producers);
    received
}

fn bench_mpsc_multi_producer(c: &mut Criterion) {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let cores = core_affinity::get_core_ids().expect("couldn't get core IDs");
    let core = cores.iter().last().expect("no cores found");
    _ = core_affinity::set_for_current(*core);

    let mut group = c.benchmark_group("mpsc_multi_producer");
    _ = group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    for (num_producers, capacity) in PRODUCERS_AND_CAPACITIES {
        let label = format!("cap_{capacity}/{num_producers}_producers");
        let _ = group.bench_function(BenchmarkId::new("local_mpsc", &label), |b| {
            b.to_async(&rt).iter(|| async {
                let local = LocalSet::new();
                let received = local
                    .run_until(async {
                        run_workload(*num_producers, *capacity, None, None).await
                    })
                    .await;
                let _ = black_box(received);
            });
        });
    }

    group.finish();

    // Same scenarios but producers yield every 32 sends, simulating a
    // workload where producers do not monopolize the executor. This
    // breaks up the tight send loop and gives the consumer more
    // interleaving opportunities, which is closer to a real pipeline
    // where process() does actual work between sends.
    //
    // Only capacities >= 256 are included here. Smaller capacities
    // with multiple producers can deadlock on unpatched code when
    // yields disrupt the tight producer-consumer cycle that the buggy
    // wakeup logic depends on.
    let mut yield_group = c.benchmark_group("mpsc_multi_producer_yield32");
    _ = yield_group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    for (num_producers, capacity) in PRODUCERS_AND_CAPACITIES {
        if *capacity < 256 {
            continue;
        }
        let label = format!("cap_{capacity}/{num_producers}_producers");
        let _ = yield_group.bench_function(BenchmarkId::new("local_mpsc", &label), |b| {
            b.to_async(&rt).iter(|| async {
                let local = LocalSet::new();
                let received = local
                    .run_until(async {
                        run_workload(*num_producers, *capacity, Some(32), None).await
                    })
                    .await;
                let _ = black_box(received);
            });
        });
    }

    yield_group.finish();

    // Producers send as fast as possible (will fill the channel and block),
    // but the consumer yields every 32 recvs. This simulates a processor
    // that does real work between reads: the channel stays near capacity,
    // senders block and get woken, but the consumer periodically gives up
    // control so woken senders can run.
    let mut consumer_yield_group =
        c.benchmark_group("mpsc_multi_producer_consumer_yield32");
    _ = consumer_yield_group.throughput(Throughput::Elements(TOTAL_MESSAGES as u64));

    for (num_producers, capacity) in PRODUCERS_AND_CAPACITIES {
        let label = format!("cap_{capacity}/{num_producers}_producers");
        let _ = consumer_yield_group.bench_function(
            BenchmarkId::new("local_mpsc", &label),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let local = LocalSet::new();
                    let received = local
                        .run_until(async {
                            run_workload(*num_producers, *capacity, None, Some(32)).await
                        })
                        .await;
                    let _ = black_box(received);
                });
            },
        );
    }

    consumer_yield_group.finish();

    // Consumer yields on every recv -- simulates a receiver that is always
    // slower than its producers, which is the realistic scenario where the
    // channel stays at capacity. Each recv gives the executor a chance to
    // run woken senders before the next pop.
    //
    // Uses fewer messages since the per-message cost is much higher with
    // a yield on every recv.
    const YIELD_EVERY_MSGS: usize = 32_768; // 1024 * 8 * 4
    let mut consumer_yield_every_group =
        c.benchmark_group("mpsc_consumer_yield_every_recv");
    _ = consumer_yield_every_group.throughput(Throughput::Elements(YIELD_EVERY_MSGS as u64));

    for (num_producers, capacity) in PRODUCERS_AND_CAPACITIES {
        let per_producer = YIELD_EVERY_MSGS / num_producers;
        if per_producer % capacity != 0 || YIELD_EVERY_MSGS % num_producers != 0 {
            continue;
        }
        let label = format!("cap_{capacity}/{num_producers}_producers");
        let np = *num_producers;
        let cap = *capacity;
        let _ = consumer_yield_every_group.bench_function(
            BenchmarkId::new("local_mpsc", &label),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let local = LocalSet::new();
                    let received = local
                        .run_until(async {
                            let (tx, rx) = mpsc::Channel::new(cap);
                            let pp = YIELD_EVERY_MSGS / np;

                            let mut producers = Vec::with_capacity(np);
                            for p in 0..np {
                                let tx_clone = tx.clone();
                                let offset = p * pp;
                                producers.push(tokio::task::spawn_local(async move {
                                    for i in 0..pp {
                                        tx_clone
                                            .send_async(offset + i)
                                            .await
                                            .expect("send must succeed");
                                    }
                                }));
                            }
                            drop(tx);

                            let yielder = PeriodicYield::new(1);
                            let consumer = tokio::task::spawn_local(async move {
                                let mut count = 0usize;
                                while let Ok(_v) = rx.recv().await {
                                    count += 1;
                                    yielder.maybe_yield().await;
                                }
                                count
                            });

                            for p in producers {
                                p.await.expect("producer task must not panic");
                            }
                            consumer.await.expect("consumer task must not panic")
                        })
                        .await;
                    let _ = black_box(received);
                });
            },
        );
    }

    consumer_yield_every_group.finish();
}

criterion_group!(benches, bench_mpsc_multi_producer);
criterion_main!(benches);
