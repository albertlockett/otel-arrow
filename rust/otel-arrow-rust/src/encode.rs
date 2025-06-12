// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::encode::record::array::ArrayOptions;
use crate::otap::{Logs, OtapBatch};
use crate::pdata::otlp::LogsVisitor;
use crate::pdata::{NoopVisitor, U64Visitor};
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::proto::opentelemetry::logs::v1::{
    LogRecordVisitable, LogRecordVisitor, LogsDataVisitable, ResourceLogsVisitable,
    ResourceLogsVisitor, ScopeLogsVisitable, ScopeLogsVisitor,
};
use crate::schema::consts;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use record::array::TimestampNanosecondArrayBuilder;
use std::sync::Arc;

mod record;

struct OtapLogEncodingVisitor {
    ts_array_builder: TimestampNanosecondArrayBuilder,
    observed_ts_array_builder: TimestampNanosecondArrayBuilder,
}

impl OtapLogEncodingVisitor {
    fn new() -> Self {
        Self {
            ts_array_builder: TimestampNanosecondArrayBuilder::new(ArrayOptions {
                nullable: false,
                dictionary_options: None,
            }),
            observed_ts_array_builder: TimestampNanosecondArrayBuilder::new(ArrayOptions {
                nullable: false,
                dictionary_options: None,
            }),
        }
    }
}

impl OtapLogEncodingVisitor {
    fn borrow_mut<'a>(&'a mut self) -> &'a mut Self {
        self
    }
}

impl LogsVisitor<()> for OtapLogEncodingVisitor {
    type Return = OtapBatch;

    fn visit_logs(mut self, mut v: impl LogsDataVisitable<()>) -> Self::Return {
        v.accept_logs_data((), &mut self);

        let mut fields = vec![];
        let mut columns = vec![];

        let ts_array = self.ts_array_builder.finish();
        if let Some(ts_array) = ts_array {
            // TODO should be nullable? idk
            fields.push(Field::new(
                consts::TIME_UNIX_NANO,
                ts_array.data_type().clone(),
                true,
            ));
            columns.push(ts_array);
        }

        if let Some(observed_ts_array) = self.observed_ts_array_builder.finish() {
            // TODO no idea if this should be nullable
            fields.push(Field::new(
                consts::OBSERVED_TIME_UNIX_NANO,
                observed_ts_array.data_type().clone(),
                true,
            ));
            columns.push(observed_ts_array);
        }

        let observed_ts_array = self.observed_ts_array_builder.finish();

        let logs_rb = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns, // TODO nounwrwap
        )
        .unwrap();

        let mut otap_batch = OtapBatch::Logs(Logs::default());
        otap_batch.set(ArrowPayloadType::Logs, logs_rb);

        return otap_batch;
    }
}

impl<Argument> ResourceLogsVisitor<Argument> for &mut OtapLogEncodingVisitor {
    fn visit_resource_logs(
        &mut self,
        arg: Argument,
        mut v: impl ResourceLogsVisitable<Argument>,
    ) -> Argument {
        v.accept_resource_logs(arg, NoopVisitor {}, self.borrow_mut(), NoopVisitor {})
    }
}

impl<Argument> ScopeLogsVisitor<Argument> for &mut OtapLogEncodingVisitor {
    fn visit_scope_logs(
        &mut self,
        arg: Argument,
        mut v: impl ScopeLogsVisitable<Argument>,
    ) -> Argument {
        v.accept_scope_logs(arg, NoopVisitor {}, self.borrow_mut(), NoopVisitor {})
    }
}

impl<Argument> LogRecordVisitor<Argument> for &mut OtapLogEncodingVisitor {
    fn visit_log_record(
        &mut self,
        arg: Argument,
        mut v: impl LogRecordVisitable<Argument>,
    ) -> Argument {
        v.accept_log_record(
            arg, // arg,
            &mut self.ts_array_builder,
            // self.borrow_mut(),
            NoopVisitor {}, // severity_number,
            NoopVisitor {}, // event_name,
            // NoopVisitor{},// observed_time_unix_nano,
            &mut self.observed_ts_array_builder,
            NoopVisitor {}, // severity_text,
            NoopVisitor {}, // body,
            NoopVisitor {}, // attributes,
            NoopVisitor {}, // dropped_attributes_count,
            NoopVisitor {}, // flags,
            NoopVisitor {}, // trace_id,
            NoopVisitor {}, // span_id
        )
    }
}

impl<Argument> U64Visitor<Argument> for &mut OtapLogEncodingVisitor {
    fn visit_u64(&mut self, arg: Argument, value: u64) -> Argument {
        println!("u64 = {:?}", value);
        arg
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pdata::otlp::LogsVisitor;
    use crate::proto::opentelemetry::common::v1::*;
    use crate::proto::opentelemetry::logs::v1::*;
    use crate::proto::opentelemetry::resource::v1::*;

    #[test]
    fn test_simple() {
        let logs = create_logs_data();
        let visitor = OtapLogEncodingVisitor::new();
        let visitable = LogsDataMessageAdapter::new(&logs);
        let result = visitor.visit_logs(&visitable);
        println!("{:?}", result);
    }

    // TODO move this to some other datagen module
    fn create_logs_data() -> LogsData {
        let mut rl: Vec<ResourceLogs> = vec![];

        for _ in [0..10] {
            let kvs = vec![
                KeyValue::new("k1", AnyValue::new_string("v1")),
                KeyValue::new("k2", AnyValue::new_string("v2")),
            ];
            let res = Resource::new(kvs.clone());
            let mut sls: Vec<ScopeLogs> = vec![];
            for _ in [0..10] {
                let is1 = InstrumentationScope::new("library");

                let mut lrs: Vec<LogRecord> = vec![];

                for _ in [0..10] {
                    let lr = LogRecord::build(2_000_000_000u64, SeverityNumber::Info, "event1")
                        .attributes(kvs.clone())
                        .finish();
                    lrs.push(lr);
                }

                let sl = ScopeLogs::build(is1.clone())
                    .log_records(lrs.clone())
                    .schema_url("http://schema.opentelemetry.io")
                    .finish();
                sls.push(sl);
            }
            rl.push(ResourceLogs::build(res).scope_logs(sls).finish())
        }

        LogsData::new(rl)
    }
}
