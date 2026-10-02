// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PromQL Processor
//!
//! Converts incoming OTAP metrics to promql-rs series format, evaluates
//! a PromQL query via the promql-rs Engine, and converts the result back
//! to OTAP gauge metrics. The output replaces the original data.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = PROMQL_PROCESSOR_URN,
    target = "otel.processor.promql",
);

pub mod convert;
pub mod to_otap;

use std::sync::Arc;

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_pdata::otap::filter::IdBitmapPool;
use otel_arrow_dfe_pdata::{OtapArrowRecords, TryIntoWithOptions};
use promql_engine::{Engine, MemorySeriesSource, RangeQuery};
use promql_engine::series;

use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;

use self::convert::{ConvertConfig, convert_metrics_batch};
use self::to_otap::series_batches_to_otap;

/// URN identifier for the PromQL processor
pub const PROMQL_PROCESSOR_URN: &str = "urn:otel:processor:promql";

/// Configuration for the PromQL processor
#[derive(serde::Deserialize)]
struct Config {
    /// The PromQL query to evaluate on incoming metrics
    query: String,
    /// The label names to expose in the series schema
    label_names: Vec<String>,
    /// Optional unit for the output gauge metric (e.g. "1/s", "By")
    #[serde(default)]
    unit: Option<String>,
    /// Optional name template for the output metric.
    /// Supports ${label_name} substitution, e.g. "rate_${__name__}".
    /// Defaults to "${__name__}" (preserves the original metric name).
    #[serde(default)]
    output_metric_name: Option<String>,
}

/// PromQL processor that evaluates a query on OTAP metrics.
struct PromqlProcessor {
    engine: Engine,
    query: String,
    convert_config: ConvertConfig,
    output_metric_name_template: Option<String>,
    unit: Option<String>,
    description: String,
    pool: IdBitmapPool,
}

/// Register PromqlProcessor as an OTAP processor factory
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Processor)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static PROMQL_PROCESSOR_FACTORY: otel_arrow_dfe_engine::ProcessorFactory<OtapPdata> =
    otel_arrow_dfe_engine::ProcessorFactory {
        name: PROMQL_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
                create_promql_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        context_declarations: None,
        wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<Config>,
    };

fn create_promql_processor(
    _pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    proc_cfg: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    let config: Config =
        serde_json::from_value(node_config.config.clone()).map_err(|e| {
            ConfigError::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;

    let mut sorted_names = config.label_names.clone();
    sorted_names.sort();

    let processor = PromqlProcessor {
        engine: Engine::new(),
        description: format!("PromQL: {}", config.query),
        query: config.query,
        convert_config: ConvertConfig {
            label_names: sorted_names,
        },
        output_metric_name_template: config.output_metric_name,
        unit: config.unit,
        pool: IdBitmapPool::new(),
    };

    Ok(ProcessorWrapper::local(
        processor,
        node,
        node_config,
        proc_cfg,
    ))
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for PromqlProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        match msg {
            Message::Control(_) => Ok(()),
            Message::PData(pdata) => {
                if pdata.signal_type() == SignalType::Metrics {
                    let (context, payload) = pdata.into_parts();
                    let arrow_records: OtapArrowRecords = payload.try_into_with_default()?;

                    let output = self.process_metrics(&arrow_records).await;
                    let pdata = OtapPdata::new(context, output.into());
                    effect_handler.send_message(pdata).await?;
                } else {
                    effect_handler.send_message(pdata).await?;
                }
                Ok(())
            }
        }
    }
}

impl PromqlProcessor {
    async fn process_metrics(&mut self, otap_batch: &OtapArrowRecords) -> OtapArrowRecords {
        // Step 1: Convert OTAP to promql-rs series
        let (series_batch, block) = match convert_metrics_batch(
            otap_batch,
            &self.convert_config,
            &mut self.pool,
        ) {
            Ok(Some(result)) => result,
            Ok(None) => {
                otel_debug!("promql_processor.no_data", query = self.query.as_str());
                return OtapArrowRecords::Metrics(Default::default());
            }
            Err(e) => {
                otel_warn!(
                    "promql_processor.convert_error",
                    error = e.to_string().as_str()
                );
                return OtapArrowRecords::Metrics(Default::default());
            }
        };

        // Step 2: Load series into a MemorySeriesSource and run the PromQL query
        let decoded = match series::decode(std::slice::from_ref(&series_batch)) {
            Ok(s) => s,
            Err(e) => {
                otel_warn!(
                    "promql_processor.decode_error",
                    error = e.as_str()
                );
                return OtapArrowRecords::Metrics(Default::default());
            }
        };

        let source = match MemorySeriesSource::try_new(decoded) {
            Ok(s) => s,
            Err(e) => {
                otel_warn!(
                    "promql_processor.source_error",
                    error = e.as_str()
                );
                return OtapArrowRecords::Metrics(Default::default());
            }
        };

        // Run an instant query at the end of the block's time range
        let query_time = block.end_ms.saturating_sub(1);
        let range = RangeQuery::new(query_time, query_time, 1_000);

        let result_batches = match self
            .engine
            .range_query_async(&source, &self.query, &range)
            .await
        {
            Ok(batches) => batches,
            Err(e) => {
                otel_warn!(
                    "promql_processor.query_error",
                    query = self.query.as_str(),
                    error = e.to_string().as_str()
                );
                return OtapArrowRecords::Metrics(Default::default());
            }
        };

        // Step 3: Convert query result back to OTAP gauge metrics
        match series_batches_to_otap(
            &result_batches,
            self.output_metric_name_template.as_deref(),
            &self.description,
            self.unit.as_deref(),
        ) {
            Ok(output) => output,
            Err(e) => {
                otel_warn!(
                    "promql_processor.deconvert_error",
                    error = e.to_string().as_str()
                );
                OtapArrowRecords::Metrics(Default::default())
            }
        }
    }
}

/// Resolve a metric name template by substituting ${label_name} placeholders
/// with actual label values from a series.
pub(crate) fn resolve_metric_name_template(
    template: &str,
    labels: &[(&str, &str)],
) -> String {
    let mut result = template.to_string();
    for &(name, value) in labels {
        let placeholder = format!("${{{name}}}");
        result = result.replace(&placeholder, value);
    }
    // Remove any unresolved placeholders
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            result.replace_range(start..start + end + 1, "");
        } else {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_substitution() {
        let labels = vec![("__name__", "http_requests"), ("method", "GET")];
        assert_eq!(
            resolve_metric_name_template("rate_${__name__}", &labels),
            "rate_http_requests"
        );
        assert_eq!(
            resolve_metric_name_template("${__name__}_by_${method}", &labels),
            "http_requests_by_GET"
        );
        assert_eq!(
            resolve_metric_name_template("${__name__}", &labels),
            "http_requests"
        );
        // Unresolved placeholders are removed
        assert_eq!(
            resolve_metric_name_template("${__name__}_${missing}", &labels),
            "http_requests_"
        );
    }
}
