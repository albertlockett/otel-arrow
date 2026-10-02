// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// Condense Attributes processor
#[cfg(feature = "condense-attributes")]
pub mod condense_attributes_processor;

/// Recordset KQL OTLP Query Engine processor
#[cfg(feature = "recordset-kql")]
pub mod recordset_kql_processor;

/// PromQL processor for evaluating PromQL queries on metrics
#[cfg(feature = "promql")]
pub mod promql_processor;

/// Resource Validator processor for validating resource attributes
#[cfg(feature = "resource-validator")]
pub mod resource_validator_processor;
