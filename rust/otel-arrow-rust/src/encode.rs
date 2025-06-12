// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::encode::record::array::boolean::{AdaptiveBooleanArrayBuilder, BooleanBuilderOptions};
use crate::encode::record::array::dictionary::DictionaryOptions;
use crate::encode::record::array::{
    ArrayAppend, ArrayAppendNulls, ArrayOptions, Int64ArrayBuilder, StringArrayBuilder,
    UInt8ArrayBuilder, UInt16ArrayBuilder,
};
use crate::otap::{Logs, OtapBatch};
use crate::otlp::attributes::store::AttributeValueType;
use crate::pdata::otlp::LogsVisitor;
use crate::pdata::{BooleanVisitor, I64Visitor, NoopVisitor, StringVisitor};
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::proto::opentelemetry::common::v1::{
    AnyValueVisitable, AnyValueVisitor, KeyValueVisitable, KeyValueVisitor,
};
use crate::proto::opentelemetry::logs::v1::{
    LogRecordVisitable, LogRecordVisitor, LogsDataVisitable, ResourceLogsVisitable,
    ResourceLogsVisitor, ScopeLogsVisitable, ScopeLogsVisitor,
};
use crate::schema::consts;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use record::array::{Int32ArrayBuilder, TimestampNanosecondArrayBuilder};
use std::sync::Arc;
use std::u16;

mod record;

struct OtapLogEncodingVisitor {
    // TODO
    // - schema_url
    // - id
    // - resource
    // - trace_id
    // - span_id
    // - scope
    // - body
    // - dropped attribute count
    // - flags
    id: UInt16ArrayBuilder,
    current_id: u16,

    timestamp: TimestampNanosecondArrayBuilder,
    observed_ts: TimestampNanosecondArrayBuilder,
    severity_number: Int32ArrayBuilder,
    severity_text: StringArrayBuilder,
    attributes: LogAttributeVisitor,
}

impl OtapLogEncodingVisitor {
    fn new() -> Self {
        // TODO double check the dict encodings before merging here ...
        Self {
            id: UInt16ArrayBuilder::new(ArrayOptions {
                nullable: true,
                dictionary_options: None,
            }),
            current_id: 0,

            timestamp: TimestampNanosecondArrayBuilder::new(ArrayOptions {
                nullable: false,
                dictionary_options: None,
            }),
            observed_ts: TimestampNanosecondArrayBuilder::new(ArrayOptions {
                nullable: false,
                dictionary_options: None,
            }),
            severity_number: Int32ArrayBuilder::new(ArrayOptions {
                nullable: true,
                dictionary_options: Some(DictionaryOptions {
                    max_cardinality: u16::MAX,
                    min_cardinality: 0,
                }),
            }),
            severity_text: StringArrayBuilder::new(ArrayOptions {
                nullable: true,
                dictionary_options: Some(DictionaryOptions {
                    max_cardinality: u16::MAX,
                    min_cardinality: 0,
                }),
            }),

            attributes: LogAttributeVisitor::new(),
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

        // TODO 1) can avoid duplicating code for every type?
        // TODO 2) need to ensure all this is in the right order?
        // TODO 3) double check which fields shouldn't be nullable

        let ids_array = self.id.finish();
        if let Some(ids_array) = ids_array {
            // TODO set metadata plain encoding
            fields.push(Field::new(consts::ID, ids_array.data_type().clone(), true));
            columns.push(ids_array);
        }

        let ts_array = self.timestamp.finish();
        if let Some(ts_array) = ts_array {
            fields.push(Field::new(
                consts::TIME_UNIX_NANO,
                ts_array.data_type().clone(),
                false,
            ));
            columns.push(ts_array);
        }

        if let Some(observed_ts_array) = self.observed_ts.finish() {
            fields.push(Field::new(
                consts::OBSERVED_TIME_UNIX_NANO,
                observed_ts_array.data_type().clone(),
                false,
            ));
            columns.push(observed_ts_array);
        }

        if let Some(severity_number) = self.severity_number.finish() {
            fields.push(Field::new(
                consts::SEVERITY_NUMBER,
                severity_number.data_type().clone(),
                true,
            ));
            columns.push(severity_number);
        }

        if let Some(severity_text) = self.severity_text.finish() {
            fields.push(Field::new(
                consts::SEVERITY_TEXT,
                severity_text.data_type().clone(),
                true,
            ));
            columns.push(severity_text);
        }

        let logs_rb = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns, // TODO nounwrwap
        )
        .unwrap();

        let mut otap_batch = OtapBatch::Logs(Logs::default());
        otap_batch.set(ArrowPayloadType::Logs, logs_rb);

        let log_attrs_batch = self.attributes.finish();
        // TODO is log attrs empty blah blah?
        otap_batch.set(ArrowPayloadType::LogAttrs, log_attrs_batch);

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
        println!("LogRecordVisitor visited");

        // set state of children
        self.attributes.current_parent_id = self.current_id;
        self.attributes.visited = false;

        let arg = v.accept_log_record(
            arg,
            &mut self.timestamp,
            &mut self.severity_number,
            NoopVisitor {}, // event_name TODO
            &mut self.observed_ts,
            &mut self.severity_text,
            NoopVisitor {}, // body TODO
            &mut self.attributes,
            // TODO rest of fields
            NoopVisitor {}, // dropped_attributes_count,
            NoopVisitor {}, // flags,
            NoopVisitor {}, // trace_id,
            NoopVisitor {}, // span_id
        );

        if self.attributes.visited {
            self.id.append_value(&self.current_id);
            self.current_id += 1;
        } else {
            self.id.append_null();
        }

        arg
    }
}

struct LogAttributeVisitor {
    visited: bool,
    current_parent_id: u16,
    parent_id: UInt16ArrayBuilder,
    keys_visitor: StringArrayBuilder,
    value_visitor: LogAttrValueVisitor,
}

impl LogAttributeVisitor {
    fn new() -> Self {
        Self {
            visited: false,
            current_parent_id: 0,
            parent_id: UInt16ArrayBuilder::new(ArrayOptions {
                nullable: false,
                dictionary_options: None,
            }),
            keys_visitor: StringArrayBuilder::new(ArrayOptions {
                nullable: true,
                dictionary_options: Some(DictionaryOptions {
                    max_cardinality: u16::MAX,
                    min_cardinality: 0,
                }),
            }),
            value_visitor: LogAttrValueVisitor::new(),
        }
    }

    fn finish(&mut self) -> RecordBatch {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(parent_id_array) = self.parent_id.finish() {
            fields.push(Field::new(
                consts::PARENT_ID,
                parent_id_array.data_type().clone(),
                false,
            ));
            columns.push(parent_id_array);
        }

        if let Some(keys_array) = self.keys_visitor.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_KEY,
                keys_array.data_type().clone(),
                false,
            ));
            columns.push(keys_array);
        }

        if let Some(type_arr) = self.value_visitor.attr_type.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_TYPE,
                type_arr.data_type().clone(),
                false,
            ));
            columns.push(type_arr);
        }

        if let Some(str_val_arr) = self.value_visitor.string_val.values.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_STR,
                str_val_arr.data_type().clone(),
                true,
            ));
            columns.push(str_val_arr);
        }

        if let Some(bool_val_arr) = self.value_visitor.bool_val.values.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_BOOL,
                bool_val_arr.data_type().clone(),
                true,
            ));
            columns.push(bool_val_arr);
        }

        if let Some(int_val_arr) = self.value_visitor.int_val.values.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_INT,
                int_val_arr.data_type().clone(),
                true,
            ));
            columns.push(int_val_arr);
        }

        // TODO no unwrap
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }
}

impl<Argument> KeyValueVisitor<Argument> for &mut LogAttributeVisitor {
    fn visit_key_value(
        &mut self,
        arg: Argument,
        mut v: impl KeyValueVisitable<Argument>,
    ) -> Argument {
        println!("LogAttributeVisitor visited");
        self.visited = true;
        self.parent_id.append_value(&self.current_parent_id);
        v.accept_key_value(arg, &mut self.keys_visitor, &mut self.value_visitor)
    }
}

struct LogAttrValueVisitor {
    attr_type: UInt8ArrayBuilder,
    string_val: AttrValueStringVisitor,
    bool_val: AttrValueBoolVisitor,
    int_val: AttributeIntVisitor,
}

impl LogAttrValueVisitor {
    fn new() -> Self {
        Self {
            attr_type: UInt8ArrayBuilder::new(ArrayOptions {
                dictionary_options: None,
                nullable: false,
            }),
            string_val: AttrValueStringVisitor::new(),
            bool_val: AttrValueBoolVisitor::new(),
            int_val: AttributeIntVisitor::new(),
        }
    }
}

impl<Argument> AnyValueVisitor<Argument> for &mut LogAttrValueVisitor {
    fn visit_any_value(
        &mut self,
        arg: Argument,
        mut v: impl AnyValueVisitable<Argument>,
    ) -> Argument {
        println!("AttributeValueVisitor visited");
        // reset all the visited flags
        self.string_val.visited = false;
        self.bool_val.visited = false;
        self.int_val.visited = false;

        let arg = v.accept_any_value(
            arg,
            &mut self.string_val,
            &mut self.bool_val,
            &mut self.int_val,
            // TODO handle other types of attributes
            NoopVisitor {}, // value_double,
            NoopVisitor {}, // value_array,
            NoopVisitor {}, // value_kvlist,
            NoopVisitor {}, // value_bytes
        );

        if self.string_val.visited {
            // TODO it's obnoxious always having to deref this stuff
            // it would be nice if the thing was copy if it would just accept the actual value
            self.attr_type
                .append_value(&(AttributeValueType::Str as u8));
            self.bool_val.values.append_null();
            self.int_val.values.append_null();
        } else if self.bool_val.visited {
            self.attr_type
                .append_value(&(AttributeValueType::Bool as u8));
            self.int_val.values.append_null();
            self.string_val.values.append_null();
        } else if self.int_val.visited {
            self.attr_type
                .append_value(&(AttributeValueType::Int as u8));
            self.string_val.values.append_null();
            self.bool_val.values.append_null();
        }

        arg
    }
}

// TODO can we remove the duplicated code for each of the attr value visitors?

struct AttrValueStringVisitor {
    visited: bool,
    values: StringArrayBuilder,
}

impl AttrValueStringVisitor {
    fn new() -> Self {
        Self {
            visited: false,
            values: StringArrayBuilder::new(ArrayOptions {
                nullable: true,
                dictionary_options: Some(DictionaryOptions {
                    max_cardinality: u16::MAX,
                    min_cardinality: u16::MAX,
                }),
            }),
        }
    }
}

impl<Argument> StringVisitor<Argument> for &mut AttrValueStringVisitor {
    fn visit_string(&mut self, arg: Argument, value: &str) -> Argument {
        println!("StringVisitor visited");
        self.visited = true;
        (&mut self.values).visit_string(arg, value)
    }
}

struct AttrValueBoolVisitor {
    visited: bool,
    values: AdaptiveBooleanArrayBuilder,
}

impl AttrValueBoolVisitor {
    fn new() -> Self {
        Self {
            visited: false,
            values: AdaptiveBooleanArrayBuilder::new(BooleanBuilderOptions { nullable: true }),
        }
    }
}

impl<Argument> BooleanVisitor<Argument> for &mut AttrValueBoolVisitor {
    fn visit_bool(&mut self, arg: Argument, value: bool) -> Argument {
        self.visited = true;
        (&mut self.values).visit_bool(arg, value)
    }
}

struct AttributeIntVisitor {
    visited: bool,
    values: Int64ArrayBuilder,
}

impl AttributeIntVisitor {
    fn new() -> Self {
        Self {
            visited: false,
            values: Int64ArrayBuilder::new(ArrayOptions {
                nullable: true,
                dictionary_options: Some(DictionaryOptions {
                    max_cardinality: u16::MAX,
                    min_cardinality: u16::MAX,
                }),
            }),
        }
    }
}

impl<Argument> I64Visitor<Argument> for &mut AttributeIntVisitor {
    fn visit_i64(&mut self, arg: Argument, value: i64) -> Argument {
        self.visited = true;
        (&mut self.values).visit_i64(arg, value)
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
        // TODO -- need to assert on the result here
        println!("{:#?}", result);
    }

    // TODO move this to some other datagen module
    fn create_logs_data() -> LogsData {
        let mut rl: Vec<ResourceLogs> = vec![];

        for _ in 0..1 {
            let mut kvs = vec![
                // KeyValue::new("k1", AnyValue::new_string("v1")),
                KeyValue::new("k2", AnyValue::new_string("v2")),
            ];
            let res = Resource::new(kvs.clone());
            let mut sls: Vec<ScopeLogs> = vec![];
            for _ in 0..1 {
                let is1 = InstrumentationScope::new("library");

                let mut lrs: Vec<LogRecord> = vec![];

                for k in 0..7 {
                    if k % 4 == 0 {
                        kvs.push(KeyValue::new("k3", AnyValue::new_int(1)));
                    }

                    if k % 5 == 0 {
                        kvs.push(KeyValue::new("k4", AnyValue::new_bool(true)));
                    }

                    let lr_builder =
                        LogRecord::build(2_000_000_000u64, SeverityNumber::Info, "event1");
                    let lr = if k % 2 == 0 {
                        lr_builder.attributes(kvs.clone())
                    } else {
                        lr_builder
                    }
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
