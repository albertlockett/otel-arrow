// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use arrow::array::RecordBatch;

use crate::error::Result;
use crate::otlp::logs::related_data::RelatedData;
use crate::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;

pub mod related_data;

pub fn logs_from(
    rb: &RecordBatch,
    related_data: &mut RelatedData
) -> Result<ExportLogsServiceRequest> {
    todo!();
}


#[cfg(test)]
mod test {

    // TODO delete this and replace with tests using the validation suite
    #[test]
    fn smoke_test() {

    }
}