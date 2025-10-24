// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for filtering code


use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use otap_df_otap::encoder::encode_logs_otap_batch;
use otap_df_otap::proto::opentelemetry::logs::v1::SeverityNumber;
use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_rust::otap::filter::logs::{LogFilter, LogMatchProperties};
use otel_arrow_rust::otap::filter::MatchType;
use otel_arrow_rust::otap::OtapArrowRecords;
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_rust::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use otel_arrow_rust::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;
use prost::Message;

fn generate_logs_batch(batch_size: usize) -> OtapArrowRecords {
    let logs = (0..batch_size)
        .map(|i| {
            let severity_number = SeverityNumber::try_from(((i % 4) * 4 + 1) as i32).unwrap();
            let severity_text = severity_number
                .as_str_name()
                .split("_")
                .skip(2)
                .next()
                .unwrap();
            let event_name = format!("{} happen", severity_text.to_lowercase());

            let attrs = vec![
                KeyValue::new("k8s.pod", AnyValue::new_string(format!("my-app-{}", i % 4))),
                KeyValue::new(
                    "k8s.ns",
                    AnyValue::new_string(format!(
                        "{}",
                        match i % 3 {
                            0 => "dev",
                            1 => "staging",
                            _ => "prod",
                        }
                    )),
                ),
                KeyValue::new(
                    "region",
                    AnyValue::new_string(if i > batch_size / 2 {
                        "us-east-1"
                    } else {
                        "us-west-1"
                    }),
                ),
            ];

            LogRecord::build(i as u64, severity_number, event_name)
                .severity_text(severity_text)
                .attributes(attrs)
                .finish()
        })
        .collect::<Vec<_>>();

    let log_req = ExportLogsServiceRequest::new(vec![
        ResourceLogs::build(Resource::default())
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope::default())
                    .log_records(logs)
                    .finish(),
            ])
            .finish(),
    ]);

    let mut bytes = vec![];
    log_req.encode(&mut bytes).expect("can encode to vec");
    let logs_view = RawLogsData::new(&bytes);
    let otap_batch = encode_logs_otap_batch(&logs_view).expect("can convert to OTAP");

    otap_batch
}

fn bench_filtering(c: &mut Criterion) {
    let batch_sizes = [32, 1024, 8192];

    // ensure it's doing the right thing
    {
        let batch = generate_logs_batch(15);
        let logs_rb = batch.get(ArrowPayloadType::Logs).unwrap();
        println!("logs");
        arrow::util::pretty::print_batches(&[logs_rb.clone()]).unwrap();
        
        let log_attrs = batch.get(ArrowPayloadType::LogAttrs).unwrap();
        println!("log_attrs");
        arrow::util::pretty::print_batches(&[log_attrs.clone()]).unwrap();

        let include = LogMatchProperties::new(
            MatchType::Strict,
            vec![],
            vec![
                // otel_arrow_rust::otap::filter::KeyValue::new(
                //     "k8s.ns".into(), 
                //     otel_arrow_rust::otap::filter::AnyValue::String("prod".into())
                // )
            ],
            vec!["WARN".into()],
            None,
            vec![]
        );
        let exclude = LogMatchProperties::new(
            MatchType::Strict,
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "TEST_AAA".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("TEST_AAA".into())
                )
            ],
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "k8s.ns".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("g".into())
                )
            ],
            vec!["BLARG".to_string()],
            None,
            vec![]
        );

        let filter = LogFilter::new(include, exclude, vec![]);
        let result = filter.filter(batch).unwrap();

        let result_logs_rb = result.get(ArrowPayloadType::Logs).unwrap();
        println!("result logs");
        arrow::util::pretty::print_batches(&[result_logs_rb.clone()]).unwrap();
        
        let result_log_attrs = result.get(ArrowPayloadType::LogAttrs).unwrap();
        println!("result log_attrs");
        arrow::util::pretty::print_batches(&[result_log_attrs.clone()]).unwrap();
    }

    let mut group = c.benchmark_group("field_filter");
    
    for batch_size in batch_sizes {
        let batch = generate_logs_batch(batch_size);
        let benchmark_id = BenchmarkId::new("batch_size=", batch_size);

        let include = LogMatchProperties::new(
            MatchType::Strict,
            vec![],
            vec![
                // otel_arrow_rust::otap::filter::KeyValue::new(
                //     "k8s.ns".into(), 
                //     otel_arrow_rust::otap::filter::AnyValue::String("prod".into())
                // )
            ],
            vec![
                "WARN".into()
            ],
            None,
            vec![]
        );
        let exclude = LogMatchProperties::new(
            MatchType::Strict,
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "TEST_AAA".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("TEST_AAA".into())
                )
            ],
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "k8s.ns".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("g".into())
                )
            ],
            vec!["BLARG".to_string()],
            None,
            vec![]
        );

        let filter = LogFilter::new(include, exclude, vec![]);

        _ = group.bench_with_input(
            benchmark_id, 
            &(batch, filter), 
            |b, input| {
                b.iter_batched(
                    || input,
                    |input| {
                        let (batch, filter) = &input;
                        let result = filter.filter(batch.clone()).unwrap();
                        black_box(result)
                    },
                    BatchSize::SmallInput
                );
            }
        )
    }

    group.finish();

    let mut group = c.benchmark_group("simple_attrs_filter");
    
    for batch_size in batch_sizes {
        let batch = generate_logs_batch(batch_size);
        let benchmark_id = BenchmarkId::new("batch_size=", batch_size);

        let include = LogMatchProperties::new(
            MatchType::Strict,
            vec![],
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "k8s.ns".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("prod".into())
                )
            ],
            vec![],
            None,
            vec![]
        );
        let exclude = LogMatchProperties::new(
            MatchType::Strict,
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "TEST_AAA".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("TEST_AAA".into())
                )
            ],
            vec![
                otel_arrow_rust::otap::filter::KeyValue::new(
                    "k8s.ns".into(), 
                    otel_arrow_rust::otap::filter::AnyValue::String("g".into())
                )
            ],
            vec!["BLARG".to_string()],
            None,
            vec![]
        );

        let filter = LogFilter::new(include, exclude, vec![]);

        _ = group.bench_with_input(
            benchmark_id, 
            &(batch, filter), 
            |b, input| {
                b.iter_batched(
                    || input,
                    |input| {
                        let (batch, filter) = &input;
                        let result = filter.filter(batch.clone()).unwrap();
                        black_box(result)
                    },
                    BatchSize::SmallInput
                );
            }
        )
    }

    group.finish();
}


#[allow(missing_docs)]
mod benches {
    use super::*;
    criterion_group!(
        name = benches;
        config = Criterion::default();
        targets = bench_filtering
    );
}
criterion_main!(benches::benches);
