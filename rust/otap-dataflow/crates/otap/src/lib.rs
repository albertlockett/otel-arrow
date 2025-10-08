// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the OTAP nodes (receiver, exporter, processor).

use crate::pdata::OtapPdata;
use otap_df_engine::{PipelineFactory, build_factory};
use otap_df_engine_macros::pipeline_factory;

/// Code for encoding OTAP batch from pdata view
pub mod encoder;
/// Implementation of OTAP Exporter that implements the exporter trait
pub mod otap_exporter;
/// gRPC service implementation
pub mod otap_grpc;
/// Implementation of OTAP Receiver that implements the receiver trait
pub mod otap_receiver;

/// This receiver receives OTLP bytes from the grpc service request and
/// produce for the pipeline OTAP PData
pub mod otlp_receiver;

/// Implementation of OTLP exporter that implements the exporter trait
pub mod otlp_exporter;

// OTAP batch processor
pub mod otap_batch_processor;

// Retry processor that is aware of the OTAP PData/context.
pub mod retry_processor;

/// Receiver that reads in syslog data
pub mod syslog_cef_receiver;

/// Generated protobuf files
pub mod proto;

pub mod pdata;

pub mod parquet_exporter;

pub mod perf_exporter;

pub mod fake_data_generator;

/// Implementation of debug processor that outputs received signals in a string format for user view
pub mod debug_processor;

/// Implementation of a noop exporter that acts as a exporter placeholder
pub mod noop_exporter;

/// testing utilities
#[cfg(test)]
mod otap_mock;
#[cfg(test)]
mod otlp_mock;

#[cfg(test)]
mod fixtures;

/// Signal-type router processor (OTAP-based)
pub mod signal_type_router;

/// Attributes processor (OTAP-based)
pub mod attributes_processor;
/// compression formats
pub mod compression;
mod metrics;
/// gRPC service implementation
pub mod otlp_grpc;

/// Factory for OTAP-based pipeline
#[pipeline_factory(OTAP, OtapPdata)]
pub static OTAP_PIPELINE_FACTORY: PipelineFactory<OtapPdata> = build_factory();


#[cfg(test)]
mod test {
    use std::fs::File;

    use arrow_ipc::writer::FileWriter;
    use arrow_ipc::reader::FileReader;
    use otel_arrow_rust::otap::OtapArrowRecords;
    use prost::Message;
    use weaver_common::result::WResult;
    use weaver_common::vdir::VirtualDirectoryPath;
    use weaver_forge::registry::ResolvedRegistry;
    use weaver_resolver::SchemaResolver;
    use weaver_semconv::registry::SemConvRegistry;
    use weaver_semconv::registry_repo::RegistryRepo;

    use crate::fake_data_generator::fake_signal::fake_otlp_logs;
    use crate::pdata::{OtapPayload, OtlpProtoBytes};

    #[tokio::test]
    async fn test_dump_fake_telemetry() {
        let registry_path = "https://github.com/open-telemetry/semantic-conventions.git[model]";
        let virtual_directory_path = VirtualDirectoryPath::try_from(registry_path.to_string()).unwrap();
        let registry_repo = RegistryRepo::try_new("main", &virtual_directory_path).unwrap();

        let semconv_specs = match SchemaResolver::load_semconv_specs(
            &registry_repo,
            true, 
            false
        ) {
            WResult::Ok(resolved_schema) => resolved_schema,
            WResult::OkWithNFEs(resolved_schema, _) => resolved_schema,
            WResult::FatalErr(e) => panic!("{e:?}")
        };

        let mut registry = SemConvRegistry::from_semconv_specs(&registry_repo, semconv_specs).unwrap();

        let resolved_schema =
            match SchemaResolver::resolve_semantic_convention_registry(&mut registry, true) {
                WResult::Ok(resolved_schema) => resolved_schema,
                WResult::OkWithNFEs(resolved_schema, _) => resolved_schema,
                WResult::FatalErr(err) => panic!("{err:?}")
            };

        let resolved_registry = ResolvedRegistry::try_from_resolved_registry(
            &resolved_schema.registry,
            resolved_schema.catalog()
        ).unwrap();

        let logs = fake_otlp_logs(10000, &resolved_registry);
        let mut log_bytes = vec![];
        logs.encode(&mut log_bytes).unwrap();

        let otap_pdata = OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(log_bytes));
        let otap_arrow_records: OtapArrowRecords = otap_pdata.try_into().unwrap();

        for payload_type in otap_arrow_records.allowed_payload_types() {
            if let Some(rb) = otap_arrow_records.get(*payload_type) {
                let file_name = format!("/tmp/{:?}.arrow", payload_type).to_ascii_lowercase();
                let file = File::create(file_name).unwrap();
                let mut writer = FileWriter::try_new(file, rb.schema_ref()).unwrap();
                writer.write(rb).unwrap();
                writer.finish().unwrap();
            }
        }
    }
}