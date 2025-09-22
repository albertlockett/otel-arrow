// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use parquet::file::properties::WriterProperties;
use serde::Deserialize;

/// Configuration of parquet exporter
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The base URI for where the parquet files should be written
    pub base_uri: String,

    /// Configuration for how to compute partitions from the dataset
    pub partitioning_strategies: Option<Vec<PartitioningStrategy>>,

    /// Options for the writer
    pub writer_options: Option<WriterOptions>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WriterOptions {
    /// Target number of rows in one parquet file. The writer will flush automatically any files
    /// that attain greater than this number of rows. If this is `None`, the writer won't flush
    /// automatically when a given file size is reached (in this case, it is best to set
    /// [`Self::flush_when_older_than`]).
    ///
    /// This is currently approximate. The writer does not currently split batches across multiple
    /// files if the cutoff for the target rows happens to be in the middle of a batch.
    ///
    /// Default = 100 million rows.
    pub target_rows_per_file: Option<usize>,

    /// If this is set, the exporter will flush files whose first batch is older than this
    /// interval. This can be used to configure the writer to flush the file before the target rows
    /// per file has been reached, which can be useful in the case that there is a desire to have
    /// the data become visible earlier. Note, setting this to too small of an interval could
    /// result in the creation of many small files, which can negatively impact read performance.
    ///
    /// Note that files may actually be buffered for slightly longer than this value. For more
    /// details see [`Self::flush_age_check_interval`]
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub flush_when_older_than: Option<Duration>,

    data_page_size_limit: Option<usize>,
    data_page_row_count_limit: Option<usize>,
    write_batch_size: Option<usize>,
    max_row_group_size: Option<usize>,
    bloom_filter_position: Option<BloomFilterPosition>,
    // writer version
    created_by: Option<String>,
    offset_index_disabled: Option<bool>,
    key_value_metadata: Option<Vec<KeyValue>>,
    default_column_properties: ColumnProperties,

    // column_properties: HashMap<ColumnPath, ColumnProperties>,
    // sorting_columns: Option<Vec<SortingColumn>>,
    column_index_truncate_length: Option<usize>,
    statistics_truncate_length: Option<usize>,
    coerce_types: Option<bool>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            flush_when_older_than: None,
            target_rows_per_file: Some(100_000_000),
            data_page_size_limit: None,
            data_page_row_count_limit: None,
            write_batch_size: None,
            bloom_filter_position: None,
            created_by: None,
            offset_index_disabled: None,
            key_value_metadata: None,
            

        }
    }
}

/// Where in the file [`ArrowWriter`](crate::arrow::arrow_writer::ArrowWriter) should
/// write Bloom filters
///
/// Basic constant, which is not part of the Thrift definition.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum BloomFilterPosition {
    /// Write Bloom Filters of each row group right after the row group
    ///
    /// This saves memory by writing it as soon as it is computed, at the cost
    /// of data locality for readers
    AfterRowGroup,
    /// Write Bloom Filters at the end of the file
    ///
    /// This allows better data locality for readers, at the cost of memory usage
    /// for writers.
    End,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: Option<String>
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ColumnProperties {
    encoding: Option<Encoding>,
    // TODO
    // codec: Option<Compression>
    dictionary_page_size_limit: Option<usize>,
    dictionary_enabled: Option<bool>,
    statistics_enabled: Option<bool>,
    write_page_header_statistics: Option<bool>,

    // TODO
    // bloom_filter_properties: Option<BloomFilterProperties>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[allow(non_camel_case_types)]
pub enum Encoding {
    /// Default byte encoding.
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as little-endian.
    /// - DOUBLE - 8 bytes per value, stored as little-endian.
    /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    PLAIN,

    /// Group packed run length encoding.
    ///
    /// Usable for definition/repetition levels encoding and boolean values.
    RLE,

    /// Delta encoding for integers, either INT32 or INT64.
    ///
    /// Works best on sorted data.
    DELTA_BINARY_PACKED,

    /// Encoding for byte arrays to separate the length values and the data.
    ///
    /// The lengths are encoded using DELTA_BINARY_PACKED encoding.
    DELTA_LENGTH_BYTE_ARRAY,

    /// Incremental encoding for byte arrays.
    ///
    /// Prefix lengths are encoded using DELTA_BINARY_PACKED encoding.
    /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
    DELTA_BYTE_ARRAY,

    /// Dictionary encoding.
    ///
    /// The ids are encoded using the RLE encoding.
    RLE_DICTIONARY,

    /// Encoding for fixed-width data.
    ///
    /// K byte-streams are created where K is the size in bytes of the data type.
    /// The individual bytes of a value are scattered to the corresponding stream and
    /// the streams are concatenated.
    /// This itself does not reduce the size of the data but can lead to better compression
    /// afterwards. Note that the use of this encoding with FIXED_LEN_BYTE_ARRAY(N) data may
    /// perform poorly for large values of N.
    BYTE_STREAM_SPLIT,
}

/// Configuration options for how the parquet files should be partitioned
#[derive(Debug, Deserialize, PartialEq)]
pub enum PartitioningStrategy {
    /// compute partition values from schema metadata keys
    #[serde(alias = "schema_metadata")]
    SchemaMetadata(Vec<String>),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize() {
        let json_cfg = "{
            \"base_uri\": \"s3://albert-bucket/parquet-files\",
            \"partitioning_strategies\": [
                {
                    \"schema_metadata\": [ \"_part_id\" ]
                }
            ],
            \"writer_options\": {
                \"target_rows_per_file\": 1000000000,
                \"flush_when_older_than\": \"5m\"
            }
        }";

        let config: Config = serde_json::from_str(json_cfg).unwrap();
        let expected = Config {
            base_uri: "s3://albert-bucket/parquet-files".to_string(),
            partitioning_strategies: Some(vec![PartitioningStrategy::SchemaMetadata(vec![
                "_part_id".to_string(),
            ])]),
            writer_options: Some(WriterOptions {
                flush_when_older_than: Some(Duration::from_secs(300)),
                target_rows_per_file: Some(1000000000),
                ..Default::default()
            }),
        };
        assert_eq!(config, expected)
    }

    #[test]
    fn test_deserialize_error_unknown_fields() {
        // this has a mistake in it where target_rows_per_file should be
        // nested w/in writer_options:
        let json_cfg = "{
            \"base_uri\": \"s3://albert-bucket/parquet-files\",
            \"partitioning_strategies\": [
                {
                    \"schema_metadata\": [ \"_part_id\" ]
                }
            ],
            \"target_rows_per_file\": 1000000000
        }";
        assert!(serde_json::from_str::<Config>(json_cfg).is_err())
    }
}
