// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::decode::record_message::RecordMessage;
use crate::error::Result;
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
    ) -> Result<(Self, Option<usize>)> {
        todo!();
    }
}