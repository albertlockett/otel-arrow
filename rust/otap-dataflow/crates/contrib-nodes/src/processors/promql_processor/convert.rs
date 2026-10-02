//! Core conversion from OTAP metrics batches to promql-rs series batches.
//!
//! Takes an `OtapArrowRecords` containing metrics and produces Arrow `RecordBatch`es
//! in the promql-rs canonical series-batch schema:
//!
//! ```text
//! labels       Struct<{name}: Utf8View, ...>   -- one field per label name, sorted
//! samples      List<Struct<
//!                  timestamp: Timestamp(Millisecond, None),
//!                  value: Float64
//!              >>
//! block_start  Timestamp(Millisecond, None)    -- inclusive, same for every row
//! block_end    Timestamp(Millisecond, None)    -- exclusive, same for every row
//! ```
//!
//! One row = one series (a label set + its samples). Labels are `Utf8View`, absent = `""`.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, ListArray, RecordBatch,
    StringViewArray, StringViewBuilder, StructArray, TimestampMillisecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{self, cast};
use arrow::datatypes::{DataType, Float64Type, Int64Type, SchemaRef};
use otel_arrow_dfe_pdata::otap::filter::IdBitmapPool;
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts;
use otel_arrow_dfe_pdata::OtapArrowRecords;

use otel_arrow_dfe_query_engine::pipeline::expr::eval::project_attrs;
use otel_arrow_dfe_query_engine::pipeline::expr::join::{
    try_build_simple_join_ids, U16IdJoinLookup, U32IdJoinLookup,
};

use promql_engine::series;
use promql_engine::series::Block;

/// Configuration for the OTAP-to-series conversion.
pub struct ConvertConfig {
    /// The label names to include in the output schema.
    /// Must include `__name__` if metric name should be a label.
    /// All other entries should be data-point attribute keys.
    pub label_names: Vec<String>,
}

/// Errors from the conversion process.
#[derive(Debug)]
pub enum ConvertError {
    /// The OTAP batch is not a Metrics batch.
    NotMetrics,
    /// Arrow compute or schema error.
    Arrow(arrow::error::ArrowError),
    /// Query engine error during joins.
    QueryEngine(String),
    /// Missing required table in the OTAP batch.
    MissingTable(ArrowPayloadType),
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertError::NotMetrics => write!(f, "expected Metrics batch"),
            ConvertError::Arrow(e) => write!(f, "arrow error: {e}"),
            ConvertError::QueryEngine(e) => write!(f, "query engine error: {e}"),
            ConvertError::MissingTable(t) => write!(f, "missing table: {t:?}"),
        }
    }
}

impl From<arrow::error::ArrowError> for ConvertError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ConvertError::Arrow(e)
    }
}

/// Convert an OTAP metrics batch into a promql-rs series RecordBatch.
///
/// Returns the series RecordBatch and the Block spanning the batch's time range.
/// Returns Ok(None) if there are no matching data points.
pub fn convert_metrics_batch(
    otap: &OtapArrowRecords,
    config: &ConvertConfig,
    pool: &mut IdBitmapPool,
) -> Result<Option<(RecordBatch, Block)>, ConvertError> {
    // -- Step a: Get root metrics batch, build selection for Gauge/Sum --
    let root = otap
        .get(ArrowPayloadType::UnivariateMetrics)
        .ok_or(ConvertError::MissingTable(
            ArrowPayloadType::UnivariateMetrics,
        ))?;

    let metric_type_col = root
        .column_by_name(consts::METRIC_TYPE)
        .expect("metric_type column required");
    let metric_type_col = metric_type_col.as_primitive::<arrow::datatypes::UInt8Type>();

    let type_mask: BooleanArray = metric_type_col
        .iter()
        .map(|v| {
            v.map(|t| t == MetricType::Gauge as u8 || t == MetricType::Sum as u8)
        })
        .collect();

    // -- Step b: Extract just the root IDs that match the type filter --
    let root_id_col = root
        .column_by_name(consts::ID)
        .expect("id column required");
    let ids_for_type = compute::filter(root_id_col, &type_mask)?;

    // -- Step c: Build IdBitmap from surviving root IDs --
    let mut root_ids_bitmap = pool.acquire();
    root_ids_bitmap
        .try_populate_from_id_column(&ids_for_type)
        .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;

    // -- Step d: Select data points whose parent_id is in the bitmap --
    let dp_batch = match otap.get(ArrowPayloadType::NumberDataPoints) {
        Some(b) if b.num_rows() > 0 => b,
        _ => {
            pool.release(root_ids_bitmap);
            return Ok(None);
        }
    };

    let dp_parent_id_col = dp_batch
        .column_by_name(consts::PARENT_ID)
        .expect("parent_id column required");
    let dp_parent_prim = dp_parent_id_col
        .as_primitive::<arrow::datatypes::UInt16Type>();

    let dp_mask: BooleanArray = dp_parent_prim
        .iter()
        .map(|v| Some(v.is_some_and(|id| root_ids_bitmap.contains(id as u32))))
        .collect();
    pool.release(root_ids_bitmap);

    if dp_mask.true_count() == 0 {
        return Ok(None);
    }

    // -- Step e: Extract only the columns we need from the data points batch, then filter --
    // TODO: This filters the entire DP batch. We only need parent_id, id,
    // time_unix_nano, int_value, and double_value. We should project first then filter,
    // or extract individual columns and filter them separately to avoid copying unused
    // columns like flags, start_time_unix_nano, etc.
    let selected_dps = compute::filter_record_batch(dp_batch, &dp_mask)?;

    // -- Step f: Join metric name onto selected data points --
    let root_id_arr = root.column_by_name(consts::ID).expect("id column");
    let root_lookup = U16IdJoinLookup::try_new_from_array(root_id_arr)
        .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;

    let dp_parent_ids = selected_dps
        .column_by_name(consts::PARENT_ID)
        .expect("parent_id");
    let take_into_root =
        try_build_simple_join_ids::<u16, { 1 << 8 }>(dp_parent_ids, &root_lookup)
            .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;

    let root_name_col = root.column_by_name(consts::NAME).expect("name column");
    let dp_metric_names = compute::take(root_name_col, &take_into_root, None)?;

    // Cast to Utf8View using the Arrow cast kernel
    let name_as_string_view = cast(&dp_metric_names, &DataType::Utf8View)?;

    // -- Step g: For each label key, extract attribute values aligned to DPs --
    let attrs_batch = otap.get(ArrowPayloadType::NumberDpAttrs);
    let dp_id_col = selected_dps.column_by_name(consts::ID);

    let mut label_columns: Vec<(String, ArrayRef)> = Vec::new();
    label_columns.push(("__name__".to_string(), name_as_string_view));

    for label_key in &config.label_names {
        if label_key == "__name__" {
            continue;
        }

        let values = if let (Some(attrs), Some(dp_ids)) = (attrs_batch, dp_id_col) {
            extract_attr_values_for_key(attrs, dp_ids, selected_dps.num_rows(), label_key)?
        } else {
            // No attrs table or no DP id column -- all empty strings
            // TODO: optimize creation of placeholder StringViewArray
            Arc::new(StringViewArray::from(vec![""; selected_dps.num_rows()])) as ArrayRef
        };

        label_columns.push((label_key.clone(), values));
    }

    // -- Step h: Build timestamps and values --
    let (timestamps_ms, values) = extract_timestamps_and_values(&selected_dps)?;

    // -- Step i: Group into series and build output --
    label_columns.sort_by(|a, b| a.0.cmp(&b.0));
    let label_names: Vec<String> = label_columns.iter().map(|(n, _)| n.clone()).collect();

    let groups = group_by_labels(&label_columns, selected_dps.num_rows());

    let schema = series::schema(&label_names);
    let (block_start_ms, block_end_ms) = compute_block_boundaries(&timestamps_ms);
    let block = Block {
        start_ms: block_start_ms,
        end_ms: block_end_ms,
    };

    let batch = build_series_batch(&schema, &label_columns, &timestamps_ms, &values, &groups, block)?;

    Ok(Some((batch, block)))
}

/// Extract attribute string values for a given key, aligned to data point rows.
///
/// Uses query-engine's `project_attrs` to filter the attributes batch by key and
/// extract the value column, then joins the result back to data point row order
/// using `U32IdJoinLookup`.
///
/// For each data point, produces the string value of the attribute with the given key,
/// or "" if the data point doesn't have that attribute.
fn extract_attr_values_for_key(
    attrs_batch: &RecordBatch,
    dp_id_col: &ArrayRef,
    num_dps: usize,
    key: &str,
) -> Result<ArrayRef, ConvertError> {
    // Use project_attrs from the query-engine to filter by key and extract [parent_id, value]
    let projected = project_attrs(attrs_batch, key, &[], true)
        .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;

    let projected = match projected {
        Some(p) => p,
        None => {
            // No attrs with this key -- return all empty strings
            // TODO: optimize creation of placeholder StringViewArray
            return Ok(Arc::new(StringViewArray::from(vec![""; num_dps])));
        }
    };

    // projected has columns [parent_id, value] where value is an AnyValue struct.
    // Extract the "str" field from the AnyValue struct for string-typed attributes.
    // TODO: handle int/double/bool attribute types by casting them to strings.
    let value_col = projected
        .column_by_name("value")
        .expect("project_attrs returns a value column");
    let value_struct = value_col.as_struct();

    let str_value = if let Some(str_col) = value_struct.column_by_name(consts::ATTRIBUTE_STR) {
        // Cast to Utf8View using the Arrow cast kernel
        cast(str_col, &DataType::Utf8View)?
    } else {
        // TODO: optimize creation of placeholder StringViewArray
        Arc::new(StringViewArray::from(vec![""; projected.num_rows()])) as ArrayRef
    };

    // Build lookup: attrs.parent_id -> row in projected
    let attrs_parent_id = projected
        .column_by_name(consts::PARENT_ID)
        .expect("parent_id in projected attrs");
    let attrs_lookup = U32IdJoinLookup::try_new_from_array(attrs_parent_id)
        .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;

    // For each DP, find matching attr row
    let take_indices =
        try_build_simple_join_ids::<u32, { 1 << 10 }>(dp_id_col, &attrs_lookup)
            .map_err(|e| ConvertError::QueryEngine(e.to_string()))?;

    // Align attr values to DP row order
    let aligned = compute::take(&str_value, &take_indices, None)?;

    // Replace nulls with "" (absent attribute = empty string in promql-rs)
    let as_sv = aligned.as_string_view();
    let mut builder = StringViewBuilder::new();
    for i in 0..as_sv.len() {
        if as_sv.is_null(i) {
            builder.append_value("");
        } else {
            builder.append_value(as_sv.value(i));
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Extract timestamps (converted to milliseconds) and values from the data points batch.
fn extract_timestamps_and_values(
    dps: &RecordBatch,
) -> Result<(Vec<i64>, Vec<f64>), ConvertError> {
    let time_col = dps
        .column_by_name(consts::TIME_UNIX_NANO)
        .expect("time_unix_nano column");
    let time_ns = time_col
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();

    let timestamps_ms: Vec<i64> = time_ns.iter().map(|v| v.unwrap_or(0) / 1_000_000).collect();

    let double_col = dps.column_by_name(consts::DOUBLE_VALUE);
    let int_col = dps.column_by_name(consts::INT_VALUE);

    let values: Vec<f64> = (0..dps.num_rows())
        .map(|i| {
            if let Some(d) = double_col {
                let d = d.as_primitive::<Float64Type>();
                if !d.is_null(i) {
                    return d.value(i);
                }
            }
            if let Some(iv) = int_col {
                let iv = iv.as_primitive::<Int64Type>();
                if !iv.is_null(i) {
                    return iv.value(i) as f64;
                }
            }
            0.0
        })
        .collect();

    Ok((timestamps_ms, values))
}

/// Group data points by their label set using dictionary-aware hashing.
///
/// Returns a list of groups, where each group is a set of row ranges.
/// Contiguous rows with the same label set are merged into a single range.
///
/// TODO: This allocates an intermediate `Vec<u64>` of per-row hashes. Could be
/// done incrementally in a single pass using a `HashMap<u64, GroupState>` that
/// tracks the current open range and extends it for consecutive rows in the same
/// group, avoiding the intermediate allocation entirely.
fn group_by_labels(
    label_columns: &[(String, ArrayRef)],
    num_rows: usize,
) -> Vec<Vec<Range<u32>>> {
    if num_rows == 0 {
        return Vec::new();
    }

    let row_hashes = compute_row_hashes(label_columns, num_rows);

    // TODO: handle hash collisions by verifying actual label values match
    let mut hash_to_group: HashMap<u64, u32> = HashMap::new();
    let mut groups: Vec<Vec<Range<u32>>> = Vec::new();
    let mut row_groups: Vec<u32> = Vec::with_capacity(num_rows);

    for &hash in row_hashes.iter() {
        let next_group_id = groups.len() as u32;
        let group_id = *hash_to_group.entry(hash).or_insert_with(|| {
            groups.push(Vec::new());
            next_group_id
        });
        row_groups.push(group_id);
    }

    // Build ranges: merge contiguous rows belonging to the same group
    let mut current_group = row_groups[0];
    let mut range_start: u32 = 0;

    for i in 1..num_rows {
        if row_groups[i] != current_group {
            groups[current_group as usize].push(range_start..i as u32);
            current_group = row_groups[i];
            range_start = i as u32;
        }
    }
    groups[current_group as usize].push(range_start..num_rows as u32);

    groups
}

/// Compute a hash for each row across all label columns.
///
/// For dictionary-encoded columns, hashes the dictionary key (index) rather than
/// the expanded value. Since all rows in a batch share the same dictionary,
/// equal keys mean equal values.
fn compute_row_hashes(label_columns: &[(String, ArrayRef)], num_rows: usize) -> Vec<u64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hashes = vec![0u64; num_rows];

    for (col_idx, (_, col)) in label_columns.iter().enumerate() {
        for row in 0..num_rows {
            let h = &mut hashes[row];
            *h = h.wrapping_mul(31).wrapping_add(col_idx as u64);

            let mut hasher = DefaultHasher::new();

            if let Some(sv) = col.as_any().downcast_ref::<StringViewArray>() {
                if sv.is_null(row) {
                    0u8.hash(&mut hasher);
                } else {
                    sv.value(row).hash(&mut hasher);
                }
            } else {
                // For dictionary-encoded or other types, cast to StringView first.
                // The cast happened in the label construction, so this shouldn't
                // be hit in normal operation. Fall back to hashing a zero.
                0u8.hash(&mut hasher);
            }

            *h = h.wrapping_mul(31).wrapping_add(hasher.finish());
        }
    }

    hashes
}

/// Compute block boundaries from the timestamp array.
fn compute_block_boundaries(timestamps_ms: &[i64]) -> (i64, i64) {
    if timestamps_ms.is_empty() {
        return (0, 1);
    }
    let min = *timestamps_ms.iter().min().unwrap();
    let max = *timestamps_ms.iter().max().unwrap();
    (min, max.saturating_add(1))
}

/// Build the final series RecordBatch in promql-rs canonical schema.
///
/// The output has one row per series (group). Each row contains:
/// - `labels`: a Struct with one Utf8View field per label name (sorted), holding
///   that series' label values (first row of the group, since all rows in a group
///   share the same labels).
/// - `samples`: a List of Struct<timestamp: Timestamp(ms), value: Float64>,
///   containing all data points for the series sorted by timestamp.
/// - `block_start` / `block_end`: constant Timestamp(ms) columns marking the
///   time range this batch covers.
fn build_series_batch(
    schema: &SchemaRef,
    label_columns: &[(String, ArrayRef)],
    timestamps_ms: &[i64],
    values: &[f64],
    groups: &[Vec<Range<u32>>],
    block: Block,
) -> Result<RecordBatch, ConvertError> {
    let num_series = groups.len();

    // Build the labels StructArray: for each series, take the first row's value
    let label_arrays: Vec<ArrayRef> = label_columns
        .iter()
        .map(|(_, col)| {
            let mut builder = StringViewBuilder::new();
            for group in groups {
                let first_row = group[0].start as usize;
                if let Some(sv) = col.as_any().downcast_ref::<StringViewArray>() {
                    if sv.is_null(first_row) {
                        builder.append_value("");
                    } else {
                        builder.append_value(sv.value(first_row));
                    }
                } else {
                    builder.append_value("");
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        })
        .collect();

    let struct_fields = match schema.field(0).data_type() {
        DataType::Struct(f) => f.clone(),
        _ => unreachable!("labels must be a struct"),
    };
    let labels = StructArray::new(struct_fields, label_arrays, None);

    // Build the samples ListArray: for each group, collect and sort (timestamp, value) pairs
    let mut all_timestamps: Vec<i64> = Vec::new();
    let mut all_values: Vec<f64> = Vec::new();
    let mut offsets: Vec<i32> = vec![0];

    for group in groups {
        let mut samples: Vec<(i64, f64)> = Vec::new();
        for range in group {
            for row in range.start..range.end {
                let row = row as usize;
                samples.push((timestamps_ms[row], values[row]));
            }
        }
        samples.sort_by_key(|&(t, _)| t);

        for (t, v) in &samples {
            all_timestamps.push(*t);
            all_values.push(*v);
        }
        offsets.push(all_timestamps.len() as i32);
    }

    let timestamp_array = TimestampMillisecondArray::from(all_timestamps);
    let value_array = Float64Array::from(all_values);
    let sample_entries = StructArray::new(
        series::sample_fields(),
        vec![Arc::new(timestamp_array), Arc::new(value_array)],
        None,
    );
    let samples = ListArray::new(
        series::sample_item(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(sample_entries),
        None,
    );

    let [block_start_col, block_end_col] = block.columns(num_series);

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(labels),
            Arc::new(samples),
            block_start_col,
            block_end_col,
        ],
    )?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use otel_arrow_dfe_pdata::otap::{Metrics, OtapBatchStore};
    use promql_engine::series;

    /// Helper to build a minimal OTAP metrics batch for testing.
    fn build_test_otap_batch() -> OtapArrowRecords {
        let mut metrics = Metrics::default();

        // Root UNIVARIATE_METRICS: gauge "http_requests" (id=0), histogram "latency" (id=1)
        {
            let id = UInt16Array::from(vec![0u16, 1]);
            let metric_type = UInt8Array::from(vec![MetricType::Gauge as u8, MetricType::Histogram as u8]);
            let name: DictionaryArray<arrow::datatypes::UInt8Type> =
                vec!["http_requests", "latency"].into_iter().collect();

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt16, false),
                Field::new("metric_type", DataType::UInt8, false),
                Field::new(
                    "name",
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                    false,
                ),
            ]));

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id), Arc::new(metric_type), Arc::new(name)])
                    .unwrap();
            metrics
                .set(ArrowPayloadType::UnivariateMetrics, batch)
                .unwrap();
        }

        // NUMBER_DATA_POINTS: 3 data points for the gauge (parent_id=0)
        {
            let parent_id = UInt16Array::from(vec![0u16, 0, 0]);
            let id = UInt32Array::from(vec![0u32, 1, 2]);
            let time = TimestampNanosecondArray::from(vec![
                1_000_000_000i64, // 1s -> 1000ms
                2_000_000_000,    // 2s -> 2000ms
                3_000_000_000,    // 3s -> 3000ms
            ]);
            let double_value = Float64Array::from(vec![10.0, 20.0, 30.0]);

            let schema = Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt16, false),
                Field::new("id", DataType::UInt32, false),
                Field::new("time_unix_nano", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
                Field::new("double_value", DataType::Float64, true),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(parent_id), Arc::new(id), Arc::new(time), Arc::new(double_value)],
            )
            .unwrap();
            metrics
                .set(ArrowPayloadType::NumberDataPoints, batch)
                .unwrap();
        }

        // NUMBER_DP_ATTRS: method=GET for DP 0,1 and method=POST for DP 2
        {
            let parent_id = UInt32Array::from(vec![0u32, 1, 2]);
            let key: DictionaryArray<arrow::datatypes::UInt8Type> =
                vec!["method", "method", "method"].into_iter().collect();
            let attr_type = UInt8Array::from(vec![1u8, 1, 1]); // 1 = String type
            let str_val: DictionaryArray<arrow::datatypes::UInt16Type> =
                vec!["GET", "GET", "POST"].into_iter().collect();

            let schema = Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt32, false),
                Field::new(
                    "key",
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                    false,
                ),
                Field::new("type", DataType::UInt8, false),
                Field::new(
                    "str",
                    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                    true,
                ),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(parent_id), Arc::new(key), Arc::new(attr_type), Arc::new(str_val)],
            )
            .unwrap();
            metrics
                .set(ArrowPayloadType::NumberDpAttrs, batch)
                .unwrap();
        }

        OtapArrowRecords::Metrics(metrics)
    }

    /// Scenario: An OTAP batch with one gauge metric and three data points is converted
    ///           to promql-rs series batches with correct grouping by label set.
    /// Guarantees: The output has the correct number of series, correct label values,
    ///             correct timestamps (ns->ms), correct sample values, and passes
    ///             promql-rs schema validation.
    #[test]
    fn test_basic_conversion() {
        let otap = build_test_otap_batch();
        let config = ConvertConfig {
            label_names: vec!["__name__".into(), "method".into()],
        };
        let mut pool = IdBitmapPool::new();

        let (batch, block) = convert_metrics_batch(&otap, &config, &mut pool)
            .unwrap()
            .expect("should produce output");

        series::validate(&batch.schema()).expect("output should pass promql-rs schema validation");

        assert_eq!(batch.num_rows(), 2, "should have 2 series: GET and POST");
        assert_eq!(block.start_ms, 1000);
        assert_eq!(block.end_ms, 3001);

        let decoded = series::decode(std::slice::from_ref(&batch)).unwrap();
        assert_eq!(decoded.len(), 2);

        let get_series = decoded.iter().find(|s| s.label("method") == "GET").unwrap();
        let post_series = decoded.iter().find(|s| s.label("method") == "POST").unwrap();

        assert_eq!(get_series.label("__name__"), "http_requests");
        assert_eq!(get_series.timestamps(), &[1000, 2000]);
        assert_eq!(get_series.values(), &[10.0, 20.0]);

        assert_eq!(post_series.label("__name__"), "http_requests");
        assert_eq!(post_series.timestamps(), &[3000]);
        assert_eq!(post_series.values(), &[30.0]);
    }

    /// Scenario: Histogram metrics are filtered out -- only Gauge/Sum produce series.
    /// Guarantees: metric_type filtering correctly excludes non-number-dp metrics.
    #[test]
    fn test_histogram_filtered_out() {
        let otap = build_test_otap_batch();
        let config = ConvertConfig {
            label_names: vec!["__name__".into()],
        };
        let mut pool = IdBitmapPool::new();

        let (batch, _) = convert_metrics_batch(&otap, &config, &mut pool)
            .unwrap()
            .unwrap();
        let decoded = series::decode(std::slice::from_ref(&batch)).unwrap();

        for s in &decoded {
            assert_eq!(s.label("__name__"), "http_requests");
        }
    }
}
