// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use arrow::array::{RecordBatch, StructArray, TimestampNanosecondArray, UInt16Array, UInt32Array};

use crate::arrays::{get_u16_array, ByteArrayAccessor, Int32ArrayAccessor, NullableArrayAccessor, StringArrayAccessor};
use crate::error::{Error, Result};
use crate::otlp::common::{ResourceArrays, ScopeArrays};
use crate::otlp::logs::related_data::RelatedData;
use crate::otlp::metrics::AppendAndGet;
use crate::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use crate::schema::consts;

pub mod related_data;

struct LogsArrays<'a> {
    id: &'a UInt16Array,
    schema_url: Option<StringArrayAccessor<'a>>,
    // time_unix_nano: Option<&'a TimestampNanosecondArray>,
    // trace_id: Option<ByteArrayAccessor<'a>>,
    // span_id: Option<ByteArrayAccessor<'a>>,
    // severity_number: Option<Int32ArrayAccessor<'a>>,
    // severity_text: Option<StringArrayAccessor<'a>>,
    // body: Option<&'a StructArray>,
    // dropped_attribute_count: Option<&'a UInt32Array>,
    // flags: Option<&'a UInt32Array>,
}

impl<'a> TryFrom<&'a RecordBatch> for LogsArrays<'a> {
    type Error = Error;

    fn try_from(rb: &'a RecordBatch) -> Result<Self> {
        let id = get_u16_array(rb, consts::ID)?;
        let schema_url = rb.column_by_name(consts::SCHEMA_URL).map(StringArrayAccessor::try_new).transpose()?;

        Ok(Self {
            id,
            schema_url,
        })
    }
}

pub fn logs_from(
    rb: &RecordBatch,
    related_data: &mut RelatedData
) -> Result<ExportLogsServiceRequest> {
    let mut logs = ExportLogsServiceRequest::default();
    let mut prev_res_id: Option<u16> = None;
    let mut prev_scope_id: Option<u16> = None;

    let mut res_id = 0;
    let mut scope_id = 0;

    let resource_arrays = ResourceArrays::try_from(rb)?;
    let scope_arrays = ScopeArrays::try_from(rb)?;
    let logs_arrays = LogsArrays::try_from(rb)?;

    for idx in 0..rb.num_rows() {
        let res_delta_id = resource_arrays.id.value_at(idx).unwrap_or_default();
        res_id += res_delta_id;

        if prev_res_id != Some(res_id) {
            // new resource id
            prev_res_id = Some(res_id);
            let resource_logs = logs.resource_logs.append_and_get();
            prev_scope_id = None;

            // Update the resource field of the current resource logs
            let resource = resource_logs.resource.get_or_insert_default();
            if let Some(dropped_attributes_count) = resource_arrays.dropped_attributes_count.value_at(idx) {
                resource.dropped_attributes_count = dropped_attributes_count;
            }

            if let Some(res_id) = resource_arrays.id.value_at(idx) && let Some(attrs) = related_data.res_attr_map_store.attribute_by_delta_id(res_id) {
                resource.attributes = attrs.to_vec();
            }

            resource_logs.schema_url = resource_arrays.schema_url.value_at(idx).unwrap_or_default();
        }

        let scope_delta_id_opt = scope_arrays.id.value_at(idx);
        scope_id = scope_delta_id_opt.unwrap_or_default();

        if prev_scope_id != Some(scope_id) {
            
        }
    };

    todo!();
}



#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use prost::Message;
    
    use crate::Consumer;
    use crate::opentelemetry::BatchArrowRecords;

    //  TODO -- this probably isn't how we want to test this, but I can't seem
    // to dream up a better way
    #[test]
    fn smoke_test() {
        let mut file = File::open("/Users/a.lockett/Desktop/otap_logs.pb").unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).unwrap();
        let mut bar = BatchArrowRecords::decode(contents.as_ref()).unwrap();

        let mut consumer = Consumer::default();
        let res = consumer.consume_logs_batches(&mut bar);
        res.unwrap();
    }
}