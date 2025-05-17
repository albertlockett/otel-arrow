// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

mod array;

struct RecordBatchBuilder {}

impl RecordBatchBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::array::string::StringArrayBuilder;
    use super::array::{ArrayBuilder, DynamicArrayBuilder, DynamicArrayBuilderConfig};

    fn smoke_test() {
        let mut string_builder =
            DynamicArrayBuilder::<StringArrayBuilder>::new(DynamicArrayBuilderConfig::default());
        string_builder.append_value(&"fuck".to_string()).unwrap();

        let strings = string_builder.finish();

        let schema = Schema::new(vec![Field::new("strings", strings.data_type, false)]);

        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![strings.array]).unwrap();
    }
}
