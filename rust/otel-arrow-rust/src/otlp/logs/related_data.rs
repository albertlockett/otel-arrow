// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::decode::record_message::RecordMessage;
use crate::error;
use crate::opentelemetry::ArrowPayloadType;
use crate::otlp::attributes::store::Attribute16Store;

#[derive(Default)]
pub struct RelatedData {
    // TODO having these record ids in related data seems like it's only useful
    // in the decoder loop. We might want to extract this to some kind of 
    // reusable delta cursor (and go fix it in metrics/related_data.rs) and also
    // in the golang code
    pub(crate) log_record_id: u16,

    pub(crate) res_attr_map_store: Attribute16Store,
    pub(crate) scope_attr_map_store: Attribute16Store,
    pub(crate) log_record_attr_map_store: Attribute16Store,
}

impl RelatedData {
    pub fn log_record_id_from_delta(&mut self, delta: u16) -> u16 {
        self.log_record_id += delta;
        self.log_record_id
    }

    // returns an instance of self and also the index of the main log record
    // in the input array.
    //
    // TODO it seems kind of hokey that this is like a constructor but it
    // also just returns this extra value (I think it's because we're doing a
    // single pass over the list of records). We might want to refactor this
    // to be a single function that passes over the records and returns the
    // indices for each arrow payload type. If we did that, we could just
    // implement the From trait for this struct
    pub fn from_record_messages(
        rbs: &[RecordMessage]
    ) -> error::Result<(Self, Option<usize>)> {
        let mut related_data = RelatedData::default();
        let mut logs_record_idx: Option<usize> = None;

        for (idx, rm) in rbs.iter().enumerate() {
            println!("SCHEMA IS {:#?}\n---", rm.record.schema());
            arrow::util::pretty::print_batches(&[rm.record.clone()]);

            match rm.payload_type {
                ArrowPayloadType::Logs => {
                    logs_record_idx = Some(idx)
                },
                ArrowPayloadType::ResourceAttrs => {
                    related_data.res_attr_map_store = Attribute16Store::try_from(&rm.record)?;
                },
                ArrowPayloadType::ScopeAttrs => {
                    related_data.scope_attr_map_store = Attribute16Store::try_from(&rm.record)?;
                }
                ArrowPayloadType::LogAttrs => {
                    related_data.log_record_attr_map_store = Attribute16Store::try_from(&rm.record)?;
                }
                _ => {
                    return error::UnsupportedPayloadTypeSnafu {
                        actual: rm.payload_type
                    }.fail()
                }
            }
        }

        return Ok((related_data, logs_record_idx))
    }
}