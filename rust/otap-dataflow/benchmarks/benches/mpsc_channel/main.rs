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
use std::hint::black_box;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

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
            for i in 0..per_producer {
                tx_clone
                    .send_async(offset + i)
                    .await
                    .expect("send must succeed");
                if let Some(interval) = yield_every {
                    if i % interval == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }
        }));
    }
    // Drop the original sender so the channel closes when all producers finish.
    drop(tx);

    let consumer = tokio::task::spawn_local(async move {
        let mut count = 0usize;
        while let Ok(_v) = rx.recv().await {
            count += 1;
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
                    .run_until(async { run_workload(*num_producers, *capacity).await })
                    .await;
                let _ = black_box(received);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_mpsc_multi_producer);
criterion_main!(benches);
