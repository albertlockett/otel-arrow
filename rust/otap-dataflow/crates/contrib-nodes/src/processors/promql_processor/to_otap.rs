// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Converts promql-rs series batches back to OTAP gauge metrics.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array,
    UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapBatchStore};
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts;
use otel_arrow_dfe_pdata::OtapArrowRecords;

use promql_engine::series::{BLOCK_END, BLOCK_START, LABELS, SAMPLES, TIMESTAMP, VALUE};

use super::convert::ConvertError;

/// Convert promql-rs series batches into OTAP gauge metrics.
///
/// The metric name for each series is resolved from `output_metric_name_template`
/// using `${label_name}` substitution. If no template is provided, the `__name__`
/// label is used directly.
pub fn series_batches_to_otap(
    batches: &[RecordBatch],
    output_metric_name_template: Option<&str>,
    description: &str,
    unit: Option<&str>,
) -> Result<OtapArrowRecords, ConvertError> {
    let mut metrics = Metrics::default();

    // Collect all series across batches
    let mut metric_name_to_id: HashMap<String, u16> = HashMap::new();
    let mut root_names: Vec<String> = Vec::new();

    let mut dp_parent_ids: Vec<u16> = Vec::new();
    let mut dp_ids: Vec<u32> = Vec::new();
    let mut dp_timestamps_ns: Vec<i64> = Vec::new();
    let mut dp_values: Vec<f64> = Vec::new();

    // TODO: cast key/str columns to dictionary encoding for better compression
    let mut attr_parent_ids: Vec<u32> = Vec::new();
    let mut attr_keys: Vec<String> = Vec::new();
    let mut attr_types: Vec<u8> = Vec::new();
    let mut attr_strs: Vec<Option<String>> = Vec::new();

    let mut next_dp_id: u32 = 0;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let labels = batch.column_by_name(LABELS).expect("canonical").as_struct();
        let samples = batch.column_by_name(SAMPLES).expect("canonical").as_list::<i32>();
        let sample_struct = samples.values().as_struct();
        let timestamps = sample_struct
            .column_by_name(TIMESTAMP)
            .expect("canonical")
            .as_primitive::<arrow::datatypes::TimestampMillisecondType>();
        let values = sample_struct
            .column_by_name(VALUE)
            .expect("canonical")
            .as_primitive::<arrow::datatypes::Float64Type>();
        let offsets = samples.value_offsets();

        for row in 0..batch.num_rows() {
            // Collect labels for this series
            let series_labels: Vec<(&str, &str)> = labels
                .fields()
                .iter()
                .zip(labels.columns())
                .map(|(field, col)| (field.name().as_str(), col.as_string_view().value(row)))
                .collect();

            // Determine metric name via template or __name__
            let name = if let Some(template) = output_metric_name_template {
                super::resolve_metric_name_template(template, &series_labels)
            } else {
                series_labels
                    .iter()
                    .find(|(k, _)| *k == "__name__")
                    .map(|(_, v)| v.to_string())
                    .unwrap_or_default()
            };

            let next_id = root_names.len() as u16;
            let metric_id = *metric_name_to_id.entry(name.clone()).or_insert_with(|| {
                root_names.push(name);
                next_id
            });

            // Extract samples for this series
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;

            // Filter to non-__name__, non-empty labels for attributes
            let attr_labels: Vec<(&str, &str)> = series_labels
                .iter()
                .filter(|(k, v)| *k != "__name__" && !v.is_empty())
                .copied()
                .collect();

            for i in start..end {
                let dp_id = next_dp_id;
                next_dp_id += 1;

                dp_parent_ids.push(metric_id);
                dp_ids.push(dp_id);
                dp_timestamps_ns.push(timestamps.value(i) * 1_000_000); // ms -> ns
                dp_values.push(values.value(i));

                // Attributes for this data point
                for &(key, val) in &attr_labels {
                    attr_parent_ids.push(dp_id);
                    attr_keys.push(key.to_string());
                    attr_types.push(1); // String type
                    attr_strs.push(Some(val.to_string()));
                }
            }
        }
    }

    if root_names.is_empty() {
        return Ok(OtapArrowRecords::Metrics(metrics));
    }

    // Build root UNIVARIATE_METRICS batch
    {
        let num_metrics = root_names.len();
        let ids: Vec<u16> = (0..num_metrics as u16).collect();
        let metric_types = vec![MetricType::Gauge as u8; num_metrics];
        let units: Vec<&str> = vec![unit.unwrap_or(""); num_metrics];
        let descriptions: Vec<&str> = vec![description; num_metrics];

        let schema = Arc::new(Schema::new(vec![
            Field::new(consts::ID, DataType::UInt16, false),
            Field::new(consts::METRIC_TYPE, DataType::UInt8, false),
            Field::new(consts::NAME, DataType::Utf8, false),
            Field::new(consts::UNIT, DataType::Utf8, true),
            Field::new(consts::DESCRIPTION, DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt16Array::from(ids)),
                Arc::new(UInt8Array::from(metric_types)),
                Arc::new(StringArray::from(root_names.clone())),
                Arc::new(StringArray::from(units)),
                Arc::new(StringArray::from(descriptions)),
            ],
        )?;
        metrics
            .set(ArrowPayloadType::UnivariateMetrics, batch)
            .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;
    }

    // Build NUMBER_DATA_POINTS batch
    if !dp_parent_ids.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(consts::PARENT_ID, DataType::UInt16, false),
            Field::new(consts::ID, DataType::UInt32, false),
            Field::new(
                consts::TIME_UNIX_NANO,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(consts::DOUBLE_VALUE, DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt16Array::from(dp_parent_ids)),
                Arc::new(UInt32Array::from(dp_ids)),
                Arc::new(TimestampNanosecondArray::from(dp_timestamps_ns)),
                Arc::new(Float64Array::from(dp_values)),
            ],
        )?;
        metrics
            .set(ArrowPayloadType::NumberDataPoints, batch)
            .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;
    }

    // Build NUMBER_DP_ATTRS batch
    // TODO: cast key and str columns to dictionary encoding for better compression
    if !attr_parent_ids.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(consts::PARENT_ID, DataType::UInt32, false),
            Field::new(consts::ATTRIBUTE_KEY, DataType::Utf8, false),
            Field::new(consts::ATTRIBUTE_TYPE, DataType::UInt8, false),
            Field::new(consts::ATTRIBUTE_STR, DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(attr_parent_ids)),
                Arc::new(StringArray::from(attr_keys)),
                Arc::new(UInt8Array::from(attr_types)),
                Arc::new(StringArray::from(attr_strs)),
            ],
        )?;
        metrics
            .set(ArrowPayloadType::NumberDpAttrs, batch)
            .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;
    }

    Ok(OtapArrowRecords::Metrics(metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_engine::series;
    use promql_engine::series::Block;

    /// Scenario: A series batch with two series is converted back to OTAP gauge metrics.
    /// Guarantees: The output has the correct number of metrics, data points,
    ///             and attributes with correct values including ns timestamps.
    #[test]
    fn test_series_to_otap_basic() {
        let s1 = series::Series::new(
            &[("__name__", "my_metric"), ("method", "GET")],
            vec![1000, 2000],
            vec![10.0, 20.0],
        )
        .unwrap();
        let s2 = series::Series::new(
            &[("__name__", "my_metric"), ("method", "POST")],
            vec![1000],
            vec![30.0],
        )
        .unwrap();

        let all = [s1, s2];
        let names = series::label_names_of(&all);
        let batch =
            series::encode(&names, &all, Block { start_ms: 1000, end_ms: 2001 }).unwrap();

        let result =
            series_batches_to_otap(&[batch], None, "PromQL: my_metric", Some("1")).unwrap();

        let root = result
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("root");
        assert_eq!(root.num_rows(), 1); // one unique metric name

        let name_col = root.column_by_name(consts::NAME).unwrap();
        assert_eq!(name_col.as_string::<i32>().value(0), "my_metric");

        let desc_col = root.column_by_name(consts::DESCRIPTION).unwrap();
        assert_eq!(desc_col.as_string::<i32>().value(0), "PromQL: my_metric");

        let unit_col = root.column_by_name(consts::UNIT).unwrap();
        assert_eq!(unit_col.as_string::<i32>().value(0), "1");

        let dps = result
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("data points");
        assert_eq!(dps.num_rows(), 3); // 2 + 1

        // Timestamps should be back in nanoseconds
        let ts = dps
            .column_by_name(consts::TIME_UNIX_NANO)
            .unwrap()
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        assert_eq!(ts.value(0), 1_000_000_000); // 1000ms -> 1e9 ns
        assert_eq!(ts.value(1), 2_000_000_000);
        assert_eq!(ts.value(2), 1_000_000_000);

        let attrs = result
            .get(ArrowPayloadType::NumberDpAttrs)
            .expect("attrs");
        // 3 data points, each with "method" attr = 3 attr rows
        assert_eq!(attrs.num_rows(), 3);
    }

    /// Scenario: output_metric_name overrides the __name__ label.
    /// Guarantees: The configured name is used instead of the label value.
    #[test]
    fn test_series_to_otap_with_output_name_override() {
        let s = series::Series::new(
            &[("__name__", "original_name"), ("env", "prod")],
            vec![1000],
            vec![42.0],
        )
        .unwrap();

        let all = [s];
        let names = series::label_names_of(&all);
        let batch =
            series::encode(&names, &all, Block { start_ms: 1000, end_ms: 1001 }).unwrap();

        let result = series_batches_to_otap(
            &[batch],
            Some("overridden_name"),
            "PromQL: test",
            None,
        )
        .unwrap();

        let root = result
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("root");
        let name_col = root.column_by_name(consts::NAME).unwrap();
        assert_eq!(name_col.as_string::<i32>().value(0), "overridden_name");

        let unit_col = root.column_by_name(consts::UNIT).unwrap();
        assert_eq!(unit_col.as_string::<i32>().value(0), "");
    }
}
