// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Quiver is an Arrow-native persistence layer for durable buffering of
//! telemetry data.
//!
//! The crate provides:
//!
//! - **Durability**: Write-ahead log (WAL) for crash recovery
//! - **Segment Storage**: Immutable Arrow IPC-based Quiver segment files with zero-copy reads
//! - **Subscription**: Multi-subscriber consumption with progress tracking
//! - **Maintenance**: Automatic cleanup of completed segments
//!
//! # Async Runtime
//!
//! Quiver uses [Tokio](https://tokio.rs) for async I/O operations. The primary
//! async APIs are:
//!
//! - [`QuiverEngine::open`] / [`QuiverEngineBuilder::build`] - async engine initialization
//! - [`QuiverEngine::ingest`] - async bundle ingestion with WAL persistence
//! - [`QuiverEngine::next_bundle`] - async bundle consumption with timeout and cancellation
//! - [`QuiverEngine::flush`] / [`QuiverEngine::shutdown`] - async segment finalization
//!
//! Synchronous alternatives like [`QuiverEngine::poll_next_bundle`] are available
//! for non-blocking polling patterns.
//!
//! # Architecture
//!
//! The [`QuiverEngine`] is the primary entry point. It coordinates:
//!
//! 1. **Ingestion**: Bundles are appended to the WAL, then accumulated in memory
//! 2. **Finalization**: When thresholds are exceeded, segments are written to disk
//! 3. **Subscription**: Subscribers consume bundles with at-least-once delivery
//! 4. **Cleanup**: Completed segments are deleted after all subscribers finish
//!
//! # Example
//!
//! This example demonstrates the consumer-side API with graceful shutdown via
//! cancellation token. The token can be triggered from a signal handler or
//! another task to wake waiting consumers immediately.
//!
//! ```no_run
//! use quiver::{QuiverEngine, QuiverConfig, DiskBudget, RetentionPolicy, SubscriberId, CancellationToken};
//! use std::sync::Arc;
//! use std::time::Duration;
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Use a durable filesystem path (not /tmp, which may be tmpfs on Linux)
//!     let data_dir = PathBuf::from("/var/lib/quiver/data");
//!     let cfg = QuiverConfig::default().with_data_dir(&data_dir);
//!
//!     // Configure disk budget (10 GB cap with backpressure).
//!     // for_config() reads segment/WAL sizes from the config and validates
//!     // that hard_cap >= wal_max + 2 * segment_target.
//!     let budget = Arc::new(DiskBudget::for_config(
//!         10 * 1024 * 1024 * 1024,  // 10 GB hard cap
//!         &cfg,
//!         RetentionPolicy::Backpressure,
//!     )?);
//!     let engine = QuiverEngine::open(cfg, budget).await?;
//!
//!     // Register and activate a subscriber
//!     let sub_id = SubscriberId::new("my-exporter")?;
//!     engine.register_subscriber(sub_id.clone())?;
//!     engine.activate_subscriber(&sub_id)?;
//!
//!     // Create a cancellation token for graceful shutdown.
//!     // In production, clone this token and trigger it from a signal handler:
//!     //   let shutdown_clone = shutdown.clone();
//!     //   tokio::spawn(async move {
//!     //       tokio::signal::ctrl_c().await.unwrap();
//!     //       shutdown_clone.cancel();
//!     //   });
//!     let shutdown = CancellationToken::new();
//!
//!     // Producer task would call: engine.ingest(&bundle).await?
//!
//!     // Consumer loop with timeout and cancellation support.
//!     // When shutdown.cancel() is called, next_bundle returns Err(Cancelled)
//!     // immediately, even if waiting for the timeout.
//!     let mut processed = 0u64;
//!     loop {
//!         match engine.next_bundle(&sub_id, Some(Duration::from_secs(5)), Some(&shutdown)).await {
//!             Ok(Some(handle)) => {
//!                 // Process the bundle payload...
//!                 processed += 1;
//!                 handle.ack();  // Acknowledge successful processing
//!             }
//!             Ok(None) => {
//!                 // Timeout - good time to run periodic maintenance.
//!                 // This cleans up segments that all subscribers have completed.
//!                 engine.maintain().await?;
//!                 eprintln!("Processed {} bundles so far", processed);
//!             }
//!             Err(e) if e.is_cancelled() => {
//!                 eprintln!("Shutdown requested, processed {} bundles", processed);
//!                 break;
//!             }
//!             Err(e) => return Err(e.into()),
//!         }
//!     }
//!
//!     // Graceful shutdown: finalize any pending segment and cleanup
//!     engine.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Features
//!
//! - `mmap` (default): Enable memory-mapped segment reads for zero-copy access
//! - `serde`: Enable serialization for configuration types
//! - `otap-dataflow-integrations`: Enable integration with otap-dataflow types

// Declare logging module first so macros are available to subsequent modules
pub(crate) mod logging;

pub mod budget;
pub mod config;
pub mod engine;
pub mod error;
pub mod record_bundle;
pub mod segment;
pub mod segment_store;
pub mod subscriber;
pub mod telemetry;
pub(crate) mod wal;

pub use budget::{BudgetConfigError, DiskBudget};
pub use config::{
    DurabilityMode, QuiverConfig, RetentionConfig, RetentionPolicy, SegmentConfig, WalConfig,
};
pub use engine::{MaintenanceStats, QuiverEngine, QuiverEngineBuilder};
pub use error::{QuiverError, Result};

pub use segment::SegmentError;
pub use segment_store::{ScanResult, SegmentReadMode, SegmentStore};
pub use subscriber::{
    BundleHandle, BundleIndex, BundleRef, RegistryCallback, RegistryConfig, SegmentProgress,
    SegmentProvider, SubscriberError, SubscriberId, SubscriberRegistry,
};
// Re-export CancellationToken for convenient use by consumers.
// This is the standard tokio-util type used for cooperative cancellation.
pub use tokio_util::sync::CancellationToken;
pub use wal::WalError;

#[cfg(test)]
mod test {
    use arrow_array::{
        DictionaryArray, Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array,
    };
    use arrow_buffer::Buffer;
    use arrow_ipc::{
        Block, FooterBuilder, MetadataVersion,
        convert::{IpcSchemaEncoder, fb_to_schema},
        reader::{FileDecoder, read_footer_length},
        root_as_footer,
        writer::{
            CompressionContext, DictionaryHandling, DictionaryTracker, IpcDataGenerator,
            IpcWriteOptions, write_message,
        },
    };
    use arrow_schema::{DataType, Field, Schema};
    use flatbuffers::FlatBufferBuilder;
    use std::io::Write;
    use std::sync::Arc;

    #[test]
    fn test_quiver_multi_dict_example() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "col_a",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "col_b",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Int64)),
                true,
            ),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(DictionaryArray::new(
                    UInt8Array::from_iter_values([0, 0, 1]),
                    Arc::new(StringArray::from_iter_values(["a", "b"])),
                )),
                Arc::new(DictionaryArray::new(
                    UInt16Array::from_iter_values([0, 0, 1]),
                    Arc::new(Int64Array::from_iter_values([506, 902])),
                )),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(DictionaryArray::new(
                    UInt8Array::from_iter_values([0, 0, 1]),
                    Arc::new(StringArray::from_iter_values(["c", "d"])),
                )),
                Arc::new(DictionaryArray::new(
                    UInt16Array::from_iter_values([0, 0, 1]),
                    Arc::new(Int64Array::from_iter_values([506, 902])),
                )),
            ],
        )
        .unwrap();

        let ipc_write_options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Resend);

        // pretend this is a file ...
        let mut ipc_bytes_destination: Vec<u8> = Vec::new();

        // everything in this next section would basically be logic for our custom IPC writer:

        // keep track of the block offsets
        let mut block_offsets: usize = 0;

        // we could add the standard IPC file header if we want ...
        //
        // fn pad_to_alignment(alignment: u8, len: usize) -> usize {
        //     let a = usize::from(alignment - 1);
        //     ((len + a) & !a) - len
        // }
        const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
        // const PADDING: [u8; {const}] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        // let pad_len = pad_to_alignment(ipc_write_options.alignment, ARROW_MAGIC.len());
        // let pad_len = pad_to_alignment(alignment, len)
        // let header_size = ARROW_MAGIC.len() + pad_len;
        // ipc_bytes_destination.write_all(&ARROW_MAGIC).unwrap();
        // ipc_bytes_destination.write_all(&PADDING[..pad_len])?;
        // block_offsets += header_size;

        let ipc_data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let mut compression_context = CompressionContext::default();

        let encoded_schema = ipc_data_gen.schema_to_bytes_with_dictionary_tracker(
            schema.as_ref(),
            &mut dictionary_tracker,
            &ipc_write_options,
        );
        let (meta, data) = write_message(
            &mut ipc_bytes_destination,
            encoded_schema,
            &ipc_write_options,
        )
        .unwrap();
        block_offsets += meta + data;

        let num_dicts = dictionary_tracker.dict_id().len();

        let mut dict_blocks = Vec::new();
        let mut rb_blocks = Vec::new();

        let mut write = |batch| {
            // do this after each batch is written (or before)...
            // this is a bit of a hack to reset dictionary tracker but make sure it has the correct
            // internal ID sequence. We could get rid of this hack by making a contribution to OSS
            // arrow_ipc crate probably
            dictionary_tracker.clear();
            for _ in 0..num_dicts {
                _ = dictionary_tracker.next_dict_id();
            }

            let (encoded_dictionaries, encoded_message) = ipc_data_gen
                .encode(
                    &batch,
                    &mut dictionary_tracker,
                    &ipc_write_options,
                    &mut compression_context,
                )
                .unwrap();

            for encoded_dictionary in encoded_dictionaries {
                let (meta, data) = write_message(
                    &mut ipc_bytes_destination,
                    encoded_dictionary,
                    &ipc_write_options,
                )
                .unwrap();

                let block = Block::new(block_offsets as i64, meta as i32, data as i64);
                dict_blocks.push(block);
                block_offsets += meta + data;
            }

            let (meta, data) = write_message(
                &mut ipc_bytes_destination,
                encoded_message,
                &ipc_write_options,
            )
            .unwrap();
            let block = Block::new(block_offsets as i64, meta as i32, data as i64);
            rb_blocks.push(block);
            block_offsets += meta + data;
        };

        write(batch1);
        write(batch2);

        // finish writer by writing footer -- Note, this doesn't have to be flatbuffers
        // but just doing same as arrow IPC files b/c why not :)
        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&dict_blocks);
        let record_batches = fbb.create_vector(&rb_blocks);

        // dictionaries are already written, so we can reset dictionary tracker to reuse for schema
        dictionary_tracker.clear();
        let schema = IpcSchemaEncoder::new()
            .with_dictionary_tracker(&mut dictionary_tracker)
            .schema_to_fb_offset(&mut fbb, schema.as_ref());

        let root = {
            let mut footer_builder = FooterBuilder::new(&mut fbb);
            footer_builder.add_version(MetadataVersion::V5);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            footer_builder.finish()
        };
        fbb.finish(root, None);
        let footer_data = fbb.finished_data();
        ipc_bytes_destination.write_all(footer_data).unwrap();
        ipc_bytes_destination
            .write_all(&(footer_data.len() as i32).to_le_bytes())
            .unwrap();
        ipc_bytes_destination.write_all(&ARROW_MAGIC).unwrap();
        ipc_bytes_destination.flush().unwrap();

        // everything in this next section would be stuff for the reader implementation
        let buffer = Buffer::from(ipc_bytes_destination);
        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
        let footer_start = trailer_start.checked_sub(footer_len).unwrap();
        let footer = root_as_footer(&buffer[footer_start..trailer_start]).unwrap();

        let schema = footer.schema().unwrap();
        let schema = fb_to_schema(schema);

        let batches: Vec<Block> = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        let dict_blocks: Vec<Block> = footer
            .dictionaries()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();
        // skipping block validation ...

        let get_batch = |index: usize| {
            // Load up a file reader with the dicts for this block .. we could cache these
            // if the get_batch method will be called multiple times for the same index.
            //
            // Note: creating a FileDecoder especially for this one index is a bit of a hack
            // it'd might be nice if maybe we could maybe read the dictionary blocks and call
            // RecordBatchDecoder try new directly.

            let mut decoder = FileDecoder::new(Arc::new(schema.clone()), footer.version());
            for block in &dict_blocks[index * num_dicts..(index + 1) * num_dicts] {
                let block_offset = block.offset() as usize;
                let block_len = (block.bodyLength() as usize)
                    .checked_add(block.metaDataLength() as usize)
                    .unwrap();
                let data = buffer.slice_with_length(block_offset, block_len);
                decoder.read_dictionary(block, &data).unwrap();
            }

            let block = &batches[index];
            let block_len = (block.bodyLength() as usize)
                .checked_add(block.metaDataLength() as usize)
                .unwrap();
            let data = buffer.slice_with_length(block.offset() as usize, block_len);
            decoder.read_record_batch(block, &data)
        };

        let batch1 = get_batch(0).unwrap().unwrap();
        println!("read batch1 back {batch1:?}");

        let batch2 = get_batch(1).unwrap().unwrap();
        println!("read batch1 back {batch2:?}");
    }
}
