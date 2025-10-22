// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for functions that transform attributes

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, DictionaryArray, PrimitiveBuilder, RecordBatch, StringBuilder, UInt16Array
};
use arrow::datatypes::{DataType, Field, Schema, UInt16Type};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use otel_arrow_rust::otap::filter::nulls_to_false;
use rand::Rng;
use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

fn build_id_filter1(
    id_column: &Arc<dyn Array>,
    id_set: &HashSet<u16>,
    match_id: bool,
) -> BooleanArray {
    let mut combined_id_filter = BooleanArray::new_null(id_column.len());
    // build id filter using the id hashset
    for id in id_set {
        let id_scalar = UInt16Array::new_scalar(*id);
        let id_filter = if match_id {
            arrow::compute::kernels::cmp::eq(id_column, &id_scalar)
                .expect("can compare uint16 id column with uint16 scalar")
        } else {
            arrow::compute::kernels::cmp::neq(id_column, &id_scalar)
                .expect("can compare uint16 id column with uint16 scalar")
        };
        combined_id_filter = arrow::compute::or_kleene(&combined_id_filter, &id_filter).unwrap();
    }
    // make sure there are no null values before we invert
    combined_id_filter = nulls_to_false(&combined_id_filter);
    // inverse because these are the ids we want to remove
    // not is safe please see https://docs.rs/arrow/latest/arrow/compute/fn.not.html
    arrow::compute::not(&combined_id_filter).expect("not doesn't fail")
}

fn build_id_filter2(
    id_column: &Arc<dyn Array>,
    id_set: &HashSet<u16>,
    match_id: bool,
) -> BooleanArray {
    let col = id_column
        .as_any()
        .downcast_ref::<UInt16Array>()
        .expect("expected UInt16Array");

    let mut builder = BooleanBuilder::with_capacity(id_column.len());

    for i in 0..col.len() {
        if col.is_null(i) {
            builder.append_value(false);
            continue;
        }
        let id = col.value(i);
        let keep = id_set.contains(&id) == match_id;
        builder.append_value(keep);
    }

    let filter = builder.finish();

    arrow::compute::not(&filter).expect("doesn't fail")
}

fn bench_filter_impls(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_id_filter");

    let mut rng = rand::rng();


    for batch_size in [128, 1024, 8192] {
        for num_ids in [5, 50, 100, 500] {
            let input = (
                Arc::new(UInt16Array::from_iter_values((0..batch_size).map(|_| rng.random_range(0..batch_size)))) as ArrayRef,
                HashSet::from_iter((0..num_ids).map(|_| rng.random_range(0..batch_size))),
            );

            let benchmark_id_param = format!("batch_size={batch_size}/num_ids={num_ids}");

            let _ = group.bench_with_input(BenchmarkId::new("current_impl", &benchmark_id_param), &input, |b, input| {
                b.iter_batched(
                    || input,
                    |input| build_id_filter1(&input.0, &input.1, true),
                    BatchSize::SmallInput,
                );
            });

            let _ = group.bench_with_input(BenchmarkId::new("boolbuilder_impl", &benchmark_id_param), &input, |b, input| {
                b.iter_batched(
                    || input,
                    |input| build_id_filter2(&input.0, &input.1, true),
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

#[allow(missing_docs)]
mod benches {
    use super::*;
    criterion_group!(
        name = benches;
        config = Criterion::default();
        targets = bench_filter_impls
    );
}
criterion_main!(benches::benches);
