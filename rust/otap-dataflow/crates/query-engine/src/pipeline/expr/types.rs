// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Utilities for identifying and coercing expression types

use crate::pipeline::expr::VALUE_COLUMN_NAME;
use crate::pipeline::planner::{DataPointContext, SignalContext, SignalKind};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::{Expr, cast};
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::{UTC_TIME_ZONE, consts};
use std::fmt;

/// Identifier of the logical type of some expression/column.
///
/// Note: This is different than the actual Arrow DataType. In many OTAP columns, the type
/// could use dictionary encoding so for example a column with the type variant
/// ExprLogicalType::String may have arrow DataType Dictionary<u8/16, Utf8> or simply Utf8.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ExprLogicalType {
    /// This type represents the type of an expression involving attribute value whose
    /// concrete type could not be determined by static analysis of the expression. The actual
    /// type may be one of String, Int64, Float64, Boolean, or Binary
    AnyValue,

    /// The type of an expression that involves an AnyValue that is at least known to be
    /// numeric. The actual type may be one of Int64 or Float64
    AnyValueNumeric,

    /// This type represents the value of an integer expression whose concrete type has not yet
    /// been determined. i.e when parsing, we may receive an expression such as `1`, and wil
    /// consider the type to be this generic unknown Int type until such time as it is used in
    /// conjunction with an expr that a known type is expected. For example `1 + severity_number`
    /// would result in static scalar `1`'s type being resolved to Int32, because that is the type
    /// of severity number.
    AnyInt,

    Boolean,
    Binary,
    FixedSizeBinary(usize),
    Float64,
    Int32,
    Int64,
    UInt8,
    UInt32,
    String,
    DurationNanoSecond,
    TimestampNanosecond,
}

impl ExprLogicalType {
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Int32 | Self::Int64 | Self::UInt8 | Self::UInt32)
    }

    fn is_signed_integer(&self) -> bool {
        matches!(self, Self::Int32 | Self::Int64)
    }

    /// Returns true if the logical type represents an unambiguous single type. This will return
    /// false if the type could be resolved to multiple different types, such is the case with
    /// variants `AnyValue`, `AnyValueNumeric` and `AnyInt`
    pub fn is_concrete(&self) -> bool {
        !matches!(self, Self::AnyValue | Self::AnyValueNumeric | Self::AnyInt)
    }

    /// Returns the bit width of integer types
    fn integer_bit_width(&self) -> Option<u8> {
        match self {
            Self::UInt8 => Some(8),
            Self::Int32 | Self::UInt32 => Some(32),
            Self::Int64 => Some(64),
            _ => None,
        }
    }

    /// return the datatype associated with this type. returns None if the type
    /// is not associated with a single datatype, such as with AnyValue* and ScalarInt
    pub fn datatype(&self) -> Option<DataType> {
        Some(match self {
            Self::Binary => DataType::Binary,
            Self::Boolean => DataType::Boolean,
            Self::FixedSizeBinary(len) => DataType::FixedSizeBinary(*len as i32),
            Self::Float64 => DataType::Float64,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::String => DataType::Utf8,
            Self::TimestampNanosecond => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TIME_ZONE.into()))
            }
            Self::DurationNanoSecond => DataType::Duration(TimeUnit::Nanosecond),
            Self::UInt32 => DataType::UInt32,
            Self::UInt8 => DataType::UInt8,

            // These types can actually be more than one arrow type, so return None
            Self::AnyValue | Self::AnyValueNumeric | Self::AnyInt => return None,
        })
    }
}

/// Error from field validation when a field is valid in the OTAP data model but not
/// valid for the current pipeline context (signal type, data point type, or attributes).
#[derive(Debug)]
pub enum FieldValidationError {
    /// Field exists but is not valid for the single signal type in context.
    InvalidForSignal {
        field: String,
        signal: SignalKind,
        valid_for: &'static [SignalKind],
    },
    /// Field exists but is not common to all signal types (intersection semantics).
    NotCommonToAllSignals {
        field: String,
        valid_for: &'static [SignalKind],
    },
    /// Field exists but is not valid for the data point type(s) in context.
    InvalidForDataPoint {
        field: String,
        context_desc: String,
        valid_for: &'static [MetricDataPointType],
    },
    /// Field is not common to all data point types (intersection semantics).
    NotCommonToAllDataPoints {
        field: String,
        valid_for: &'static [MetricDataPointType],
    },
}

impl fmt::Display for FieldValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidForSignal {
                field,
                signal,
                valid_for,
            } => {
                let valid_strs: Vec<&str> = valid_for.iter().map(SignalKind::as_str).collect();
                write!(
                    f,
                    "field '{field}' is not valid for {signal} pipeline; \
                     it is only available in {valid}",
                    valid = valid_strs.join(", "),
                )
            }
            Self::NotCommonToAllSignals { field, valid_for } => {
                let valid_strs: Vec<&str> = valid_for.iter().map(SignalKind::as_str).collect();
                write!(
                    f,
                    "field '{field}' is not valid in a mixed signals pipeline; \
                     it is only available in {valid} -- consider using a type-specific source \
                     or narrowing with 'if (is <Type>) {{ ... }}'",
                    valid = valid_strs.join(", "),
                )
            }
            Self::InvalidForDataPoint {
                field,
                context_desc,
                valid_for,
            } => {
                let valid_strs: Vec<&str> =
                    valid_for.iter().map(MetricDataPointType::as_str).collect();
                write!(
                    f,
                    "field '{field}' is not valid for {context_desc} data points; \
                     it is only available on {valid}",
                    valid = valid_strs.join(", "),
                )
            }
            Self::NotCommonToAllDataPoints { field, valid_for } => {
                let valid_strs: Vec<&str> =
                    valid_for.iter().map(MetricDataPointType::as_str).collect();
                write!(
                    f,
                    "field '{field}' is not valid for all data point types; \
                     it is only available on {valid}",
                    valid = valid_strs.join(", "),
                )
            }
        }
    }
}

impl SignalKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Metrics => "metrics",
            Self::Traces => "traces",
        }
    }
}

impl MetricDataPointType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::NumberDataPoint => "number data points",
            Self::HistogramDataPoint => "histogram data points",
            Self::ExponentialHistogramDataPoint => "exponential histogram data points",
            Self::SummaryDataPoint => "summary data points",
        }
    }
}

/// Return the type for a root OTAP signal field without signal-context validation.
///
/// This is used at runtime when the field has already been validated during planning.
/// Returns `None` if the field is completely unknown in the OTAP data model.
pub fn root_field_type_unvalidated(field_name: &str) -> Option<ExprLogicalType> {
    let (field_type, _valid_for) = match field_name {
        consts::SCHEMA_URL => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::DROPPED_ATTRIBUTES_COUNT => (ExprLogicalType::UInt32, ALL_SIGNALS_STATIC),
        consts::TIME_UNIX_NANO => (ExprLogicalType::TimestampNanosecond, ALL_SIGNALS_STATIC),
        consts::OBSERVED_TIME_UNIX_NANO => {
            (ExprLogicalType::TimestampNanosecond, ALL_SIGNALS_STATIC)
        }
        consts::TRACE_ID => (ExprLogicalType::FixedSizeBinary(16), ALL_SIGNALS_STATIC),
        consts::SPAN_ID => (ExprLogicalType::FixedSizeBinary(8), ALL_SIGNALS_STATIC),
        consts::FLAGS => (ExprLogicalType::UInt32, ALL_SIGNALS_STATIC),
        consts::SEVERITY_NUMBER => (ExprLogicalType::Int32, ALL_SIGNALS_STATIC),
        consts::SEVERITY_TEXT => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::EVENT_NAME => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::BODY => (ExprLogicalType::AnyValue, ALL_SIGNALS_STATIC),
        consts::DURATION_TIME_UNIX_NANO => {
            (ExprLogicalType::DurationNanoSecond, ALL_SIGNALS_STATIC)
        }
        consts::TRACE_STATE => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::PARENT_SPAN_ID => (ExprLogicalType::FixedSizeBinary(8), ALL_SIGNALS_STATIC),
        consts::KIND => (ExprLogicalType::Int32, ALL_SIGNALS_STATIC),
        consts::DROPPED_EVENTS_COUNT => (ExprLogicalType::UInt32, ALL_SIGNALS_STATIC),
        consts::DROPPED_LINKS_COUNT => (ExprLogicalType::UInt32, ALL_SIGNALS_STATIC),
        consts::METRIC_TYPE => (ExprLogicalType::UInt8, ALL_SIGNALS_STATIC),
        consts::NAME => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::DESCRIPTION => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::UNIT => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        consts::AGGREGATION_TEMPORALITY => (ExprLogicalType::Int32, ALL_SIGNALS_STATIC),
        VALUE_COLUMN_NAME => (ExprLogicalType::AnyValue, ALL_SIGNALS_STATIC),
        consts::ATTRIBUTE_KEY => (ExprLogicalType::String, ALL_SIGNALS_STATIC),
        _ => return None,
    };
    Some(field_type)
}

// Placeholder to keep the match arms consistent (unused second element).
const ALL_SIGNALS_STATIC: &[SignalKind] = ALL_SIGNALS;

use SignalKind::{Logs, Metrics, Traces};

const ALL_SIGNALS: &[SignalKind] = &[Logs, Metrics, Traces];
const LOGS_ONLY: &[SignalKind] = &[Logs];
const TRACES_ONLY: &[SignalKind] = &[Traces];
const METRICS_ONLY: &[SignalKind] = &[Metrics];
const LOGS_TRACES: &[SignalKind] = &[Logs, Traces];
const METRICS_TRACES: &[SignalKind] = &[Metrics, Traces];

/// Return the type and signal-kind scope for a root OTAP signal field.
///
/// Returns `Ok(Some(type))` if the field is valid for the given signal context.
/// Returns `Ok(None)` if the field is completely unknown in the OTAP data model.
/// Returns `Err(FieldValidationError)` if the field exists in the OTAP data model but
/// is not valid for the signal type(s) in the current pipeline context.
pub fn root_field_type(
    field_name: &str,
    signal_context: &SignalContext,
) -> Result<Option<ExprLogicalType>, FieldValidationError> {
    let (field_type, valid_for) = match field_name {
        // fields common to all signal types
        consts::SCHEMA_URL => (ExprLogicalType::String, ALL_SIGNALS),
        consts::DROPPED_ATTRIBUTES_COUNT => (ExprLogicalType::UInt32, ALL_SIGNALS),

        // logs + traces common fields
        consts::TIME_UNIX_NANO => (ExprLogicalType::TimestampNanosecond, LOGS_TRACES),
        consts::OBSERVED_TIME_UNIX_NANO => (ExprLogicalType::TimestampNanosecond, LOGS_TRACES),
        consts::TRACE_ID => (ExprLogicalType::FixedSizeBinary(16), LOGS_TRACES),
        consts::SPAN_ID => (ExprLogicalType::FixedSizeBinary(8), LOGS_TRACES),
        consts::FLAGS => (ExprLogicalType::UInt32, LOGS_TRACES),

        // logs-only fields
        consts::SEVERITY_NUMBER => (ExprLogicalType::Int32, LOGS_ONLY),
        consts::SEVERITY_TEXT => (ExprLogicalType::String, LOGS_ONLY),
        consts::EVENT_NAME => (ExprLogicalType::String, LOGS_ONLY),
        consts::BODY => (ExprLogicalType::AnyValue, LOGS_ONLY),

        // traces-only fields
        consts::DURATION_TIME_UNIX_NANO => (ExprLogicalType::DurationNanoSecond, TRACES_ONLY),
        consts::TRACE_STATE => (ExprLogicalType::String, TRACES_ONLY),
        consts::PARENT_SPAN_ID => (ExprLogicalType::FixedSizeBinary(8), TRACES_ONLY),
        consts::KIND => (ExprLogicalType::Int32, TRACES_ONLY),
        consts::DROPPED_EVENTS_COUNT => (ExprLogicalType::UInt32, TRACES_ONLY),
        consts::DROPPED_LINKS_COUNT => (ExprLogicalType::UInt32, TRACES_ONLY),

        // metrics-only fields
        consts::METRIC_TYPE => (ExprLogicalType::UInt8, METRICS_ONLY),
        consts::DESCRIPTION => (ExprLogicalType::String, METRICS_ONLY),
        consts::UNIT => (ExprLogicalType::String, METRICS_ONLY),
        consts::AGGREGATION_TEMPORALITY => (ExprLogicalType::Int32, METRICS_ONLY),

        // metrics + traces shared fields
        consts::NAME => (ExprLogicalType::String, METRICS_TRACES),

        // attribute-mode virtual columns -- not valid in signal context
        VALUE_COLUMN_NAME | consts::ATTRIBUTE_KEY => return Ok(None),

        // completely unknown field
        _ => return Ok(None),
    };

    validate_signal_field(field_name, field_type, valid_for, signal_context)
}

/// Validates whether a signal field is valid for the current signal context.
fn validate_signal_field(
    field_name: &str,
    field_type: ExprLogicalType,
    valid_for: &'static [SignalKind],
    signal_context: &SignalContext,
) -> Result<Option<ExprLogicalType>, FieldValidationError> {
    match signal_context {
        SignalContext::All => {
            // Intersection semantics: only accept fields valid for all signal types
            if valid_for.len() >= ALL_SIGNALS.len() {
                Ok(Some(field_type))
            } else {
                Err(FieldValidationError::NotCommonToAllSignals {
                    field: field_name.to_string(),
                    valid_for,
                })
            }
        }
        SignalContext::Single(kind) => {
            if valid_for.contains(kind) {
                Ok(Some(field_type))
            } else {
                Err(FieldValidationError::InvalidForSignal {
                    field: field_name.to_string(),
                    signal: *kind,
                    valid_for,
                })
            }
        }
    }
}

/// Return the type for an attribute pipeline field.
///
/// Only `key` and `value` are valid fields when operating inside `apply attributes { ... }`.
/// Returns `None` if the field is not valid for attribute pipelines.
pub fn attribute_field_type(field_name: &str) -> Option<ExprLogicalType> {
    match field_name {
        consts::ATTRIBUTE_KEY => Some(ExprLogicalType::String),
        VALUE_COLUMN_NAME => Some(ExprLogicalType::AnyValue),
        _ => None,
    }
}

use MetricDataPointType::{
    ExponentialHistogramDataPoint as ExpHistDP, HistogramDataPoint as HistDP,
    NumberDataPoint as NumDP, SummaryDataPoint as SumDP,
};

const ALL_DP_TYPES: &[MetricDataPointType] = &[NumDP, HistDP, ExpHistDP, SumDP];
const NUM_DP_ONLY: &[MetricDataPointType] = &[NumDP];
const HIST_EXPHIST_SUMMARY: &[MetricDataPointType] = &[HistDP, ExpHistDP, SumDP];
const HIST_EXPHIST: &[MetricDataPointType] = &[HistDP, ExpHistDP];
const HIST_ONLY: &[MetricDataPointType] = &[HistDP];
const EXPHIST_ONLY: &[MetricDataPointType] = &[ExpHistDP];
const SUMMARY_ONLY: &[MetricDataPointType] = &[SumDP];

/// Return the type for a metric data point field, validated against the data point context.
///
/// Returns `Ok(Some(type))` if the field is valid for the given data point context.
/// Returns `Ok(None)` if the field is completely unknown as a data point field.
/// Returns `Err(FieldValidationError)` if the field exists on some data point types
/// but is not valid for the data point type(s) in the current context.
pub fn data_point_field_type(
    field_name: &str,
    dp_context: &DataPointContext,
) -> Result<Option<ExprLogicalType>, FieldValidationError> {
    let (field_type, valid_for) = match field_name {
        // fields common to all data point types
        consts::START_TIME_UNIX_NANO => (ExprLogicalType::TimestampNanosecond, ALL_DP_TYPES),
        consts::TIME_UNIX_NANO => (ExprLogicalType::TimestampNanosecond, ALL_DP_TYPES),
        consts::FLAGS => (ExprLogicalType::UInt32, ALL_DP_TYPES),
        consts::ID => (ExprLogicalType::UInt32, ALL_DP_TYPES),
        consts::PARENT_ID => (ExprLogicalType::UInt32, ALL_DP_TYPES),

        // number data point only
        consts::INT_VALUE => (ExprLogicalType::Int64, NUM_DP_ONLY),
        consts::DOUBLE_VALUE => (ExprLogicalType::Float64, NUM_DP_ONLY),

        // histogram + exp histogram + summary (shared column name "count")
        consts::HISTOGRAM_COUNT => (ExprLogicalType::Int64, HIST_EXPHIST_SUMMARY),

        // histogram + exp histogram + summary (shared column name "sum")
        consts::HISTOGRAM_SUM => (ExprLogicalType::Float64, HIST_EXPHIST_SUMMARY),

        // histogram + exp histogram only
        consts::HISTOGRAM_MIN => (ExprLogicalType::Float64, HIST_EXPHIST),
        consts::HISTOGRAM_MAX => (ExprLogicalType::Float64, HIST_EXPHIST),

        // histogram only
        consts::HISTOGRAM_BUCKET_COUNTS => (ExprLogicalType::Int64, HIST_ONLY),
        consts::HISTOGRAM_EXPLICIT_BOUNDS => (ExprLogicalType::Float64, HIST_ONLY),

        // exponential histogram only
        consts::EXP_HISTOGRAM_SCALE => (ExprLogicalType::Int32, EXPHIST_ONLY),
        consts::EXP_HISTOGRAM_ZERO_COUNT => (ExprLogicalType::Int64, EXPHIST_ONLY),
        consts::EXP_HISTOGRAM_ZERO_THRESHOLD => (ExprLogicalType::Float64, EXPHIST_ONLY),

        // summary only
        consts::SUMMARY_QUANTILE_VALUES => (ExprLogicalType::Float64, SUMMARY_ONLY),

        _ => return Ok(None),
    };

    validate_dp_field(field_name, field_type, valid_for, dp_context)
}

/// Validates whether a data point field is valid for the current data point context.
fn validate_dp_field(
    field_name: &str,
    field_type: ExprLogicalType,
    valid_for: &'static [MetricDataPointType],
    dp_context: &DataPointContext,
) -> Result<Option<ExprLogicalType>, FieldValidationError> {
    match dp_context {
        DataPointContext::All => {
            // Intersection semantics: only accept fields valid for all data point types
            if valid_for.len() >= ALL_DP_TYPES.len() {
                Ok(Some(field_type))
            } else {
                Err(FieldValidationError::NotCommonToAllDataPoints {
                    field: field_name.to_string(),
                    valid_for,
                })
            }
        }
        DataPointContext::Single(dp_type) => {
            if valid_for.contains(dp_type) {
                Ok(Some(field_type))
            } else {
                Err(FieldValidationError::InvalidForDataPoint {
                    field: field_name.to_string(),
                    context_desc: dp_type.as_str().to_string(),
                    valid_for,
                })
            }
        }
    }
}

/// Returns true if the field on the root batch can be a dictionary encoded type
pub fn root_field_supports_dict_encoding(field_name: &str) -> bool {
    // TODO - when we have better support for time arithmetic we should test that this
    // duration type gets coerced into a dictionary during assignment for column with name
    // consts::DURATION_TIME_UNIX_NANO

    matches!(
        field_name,
        consts::SCHEMA_URL
            | consts::TRACE_ID
            | consts::SPAN_ID
            | consts::SEVERITY_NUMBER
            | consts::SEVERITY_TEXT
            | consts::EVENT_NAME
            | consts::TRACE_STATE
            | consts::KIND
            | consts::NAME
            | consts::DESCRIPTION
            | consts::UNIT
            | consts::AGGREGATION_TEMPORALITY
    )
}

/// Return the type from a nested struct field on the root OTAP record batch such as resource/scope
///
/// Returns None if the field is not known in the OTAP data model.
pub fn nested_struct_field_type(field_name: &str) -> Option<ExprLogicalType> {
    Some(match field_name {
        // resource fields
        consts::SCHEMA_URL => ExprLogicalType::String,

        // scope fields
        consts::NAME => ExprLogicalType::String,
        consts::VERSION => ExprLogicalType::String,

        // common fields
        consts::DROPPED_ATTRIBUTES_COUNT => ExprLogicalType::UInt32,

        _ => return None,
    })
}

/// Coerce two integer types to a common type for arithmetic operations.
/// Rules:
/// - If either type is signed, result is signed
/// - Result has the larger bit width of the two types
/// - Special case: UInt32 + any signed type -> Int64 (to avoid overflow, since UInt32 max > Int32 max)
/// - UInt8 + Int32 -> Int32 (signed wins, larger width sufficient)
/// - UInt8 + UInt32 -> UInt32 (both unsigned, larger width)
fn coerce_integer_types(left: &ExprLogicalType, right: &ExprLogicalType) -> ExprLogicalType {
    let left_signed = left.is_signed_integer();
    let right_signed = right.is_signed_integer();
    let left_width = left.integer_bit_width().expect("left is integer");
    let right_width = right.integer_bit_width().expect("right is integer");

    let any_signed = left_signed || right_signed;
    let has_uint32 =
        matches!(left, ExprLogicalType::UInt32) || matches!(right, ExprLogicalType::UInt32);

    // Special case: if mixing UInt32 with any signed type, must use Int64
    // because UInt32's max value (~4,2 million) doesn't fit in Int32
    if any_signed && has_uint32 {
        return ExprLogicalType::Int64;
    }

    let max_width = left_width.max(right_width);

    match (any_signed, max_width) {
        // If any is signed, use signed type with appropriate width
        (true, w) if w <= 32 => ExprLogicalType::Int32,
        (true, _) => ExprLogicalType::Int64,
        // Both unsigned
        (false, w) if w <= 8 => ExprLogicalType::UInt8,
        (false, w) if w <= 32 => ExprLogicalType::UInt32,
        // Note: we don't have UInt64, so this shouldn't happen with current types
        (false, _) => ExprLogicalType::UInt32,
    }
}

/// Adds a cast logical expression to cast the value of the expression to the passed data type.
///
/// This is used when coercing the input types for expression operations.
pub fn cast_expr(expr: &mut Expr, data_type: DataType) {
    *expr = cast(std::mem::take(expr), data_type)
}

/// Attempt to determine the type of the result of an arithmetic expression performed on the passed
/// left and right arguments.
///
/// This function will also coerce either the left or right side into a type that is compatible
/// with the other side for arithmetic operation, by adding casts in the logical expression tree.
///
/// Type coercion rules for integer arithmetic:
/// - Same types: No coercion (e.g., UInt8 + UInt8 -> UInt8)
/// - Both unsigned: Coerce to larger bit width (e.g., UInt8 + UInt32 -> UInt32)
/// - Both signed: Coerce to larger bit width (e.g., Int32 + Int64 -> Int64)
/// - Mixed signedness with UInt32: Always use Int64 to avoid overflow (e.g., UInt32 + Int32 -> Int64)
/// - Mixed signedness without UInt32: Use signed type with larger width (e.g., UInt8 + Int32 -> Int32)
/// - Unresolved scalar integers: Coerced to match the concrete type on the other side
/// - AnyValue with integers: Coerced to Int64 (the only integer type AnyValue can represent)
///
/// Returns the type the arithmetic operation will produce IF it were to evaluate successfully.
/// This returns None if it can be detected that arithmetic can be performed on the passed types.
///
/// However, also note that just because this function returns Some(type), does not automatically
/// mean that the expression evaluation will succeed. It only indicates that if the expression
/// evaluation were to succeed, its result would be of the returned type.
///
/// For example, consider an expression such as `attributes["x"] + 1`. Because we're adding an
/// integer to an `AnyValue`, and we know the only integer type an AnyValue can take on is Int64,
/// this function will return `Some(Int64)`. However, if at runtime `attributes["x"]` turns out to
/// not be an Int64 type attribute, the expression evaluation will fail.
pub fn coerce_arithmetic(
    left_expr: &mut Expr,
    left_type: &mut ExprLogicalType,
    right_expr: &mut Expr,
    right_type: &mut ExprLogicalType,
) -> Option<ExprLogicalType> {
    // TODO - need to update the rules here when we support date/time/duration arithmetic
    match &*left_type {
        ExprLogicalType::AnyValue | ExprLogicalType::AnyValueNumeric => {
            // The left side of the arithmetic operation is an AnyValue, or AnyValue numeric. The
            // only way the arithmetic will succeed at runtime is if the left side is either Int or
            // Double variant of AnyValue.
            //
            // We proceed assuming the left side is one of these types, and return a type only if
            // the right side is, or can be converted to, a type that can successfully do
            // arithmetic arithmetic operation with one of these possible types...

            match &*right_type {
                ExprLogicalType::AnyValue | ExprLogicalType::AnyValueNumeric => {
                    // we're adding two AnyValues, but we don't know they're types. We'll have to
                    // assume the types can be added, and let it produce a runtime error if types
                    // were not compatible. The evaluation will succeed if both sides are either
                    // Int or Double.
                    *left_type = ExprLogicalType::AnyValueNumeric;
                    *right_type = ExprLogicalType::AnyValueNumeric;
                    Some(ExprLogicalType::AnyValueNumeric)
                }

                // If the right side is one of our expected AnyValue variants, we know the
                // expression will only succeed if the left side was the same type. No need to
                // coerce the expressions, but we've discovered what the type of the result will be
                ExprLogicalType::Float64 => {
                    *left_type = ExprLogicalType::Float64;
                    Some(ExprLogicalType::Float64)
                }
                ExprLogicalType::Int64 => {
                    *left_type = ExprLogicalType::Int64;
                    Some(ExprLogicalType::Int64)
                }

                ExprLogicalType::AnyInt => {
                    // default type scalar int is int64, and the only type for AnyValue that is int
                    //  like is int64. We don't need to massage the input types, but we've
                    // identified what the expression output of the expression assuming evaluation
                    // succeeds
                    *left_type = ExprLogicalType::Int64;
                    Some(ExprLogicalType::Int64)
                }

                other if other.is_integer() => {
                    // TODO - this is probably controversial. We might want to force users to do an
                    // explicit cast when adding different integer types.
                    //
                    // we have a different type of integer value. automatically cast it to int64 so
                    // addition will succeed
                    *left_type = ExprLogicalType::Int64;
                    cast_expr(right_expr, DataType::Int64);
                    *right_type = ExprLogicalType::Int64;

                    Some(ExprLogicalType::Int64)
                }

                _ => {
                    // other types cannot be added to AnyValue
                    None
                }
            }
        }
        ExprLogicalType::AnyInt => match &*right_type {
            // The left side is a scalar int type. We initialize these to be an int64 in the
            // expression planner, but this is just a placeholder until if/when we know the
            // actual type that will be required.
            ExprLogicalType::Int64 => {
                // nothing to do, types are already aligned
                Some(ExprLogicalType::Int64)
            }
            ExprLogicalType::AnyValue | ExprLogicalType::AnyValueNumeric => {
                // coerce any value into the integer variant
                *right_type = ExprLogicalType::Int64;
                Some(ExprLogicalType::Int64)
            }
            right_int_type if right_int_type.is_integer() => {
                // safety: this should always return Some because we can always determine the
                // logical arrow data type for integer types
                let arrow_data_type = right_int_type.datatype().expect("single data type");
                cast_expr(left_expr, arrow_data_type);
                *left_type = right_int_type.clone();
                Some(right_int_type.clone())
            }
            _ => {
                // other types cannot be integer types
                None
            }
        },
        ExprLogicalType::Float64 => match &*right_type {
            ExprLogicalType::Float64 => {
                // nothing to do, types already aligned
                Some(ExprLogicalType::Float64)
            }
            ExprLogicalType::AnyValue | ExprLogicalType::AnyValueNumeric => {
                // coerce any value into the integer variant
                *right_type = ExprLogicalType::Float64;
                Some(ExprLogicalType::Float64)
            }
            _ => {
                // other types cannot be float types
                None
            }
        },
        left_int_type if left_int_type.is_integer() => match &*right_type {
            ExprLogicalType::AnyValue => {
                // cast the left side to int64, as this is the only integer type that the AnyValue
                // type can take on
                cast_expr(left_expr, DataType::Int64);
                *left_type = ExprLogicalType::Int64;
                *right_type = ExprLogicalType::Int64;
                Some(ExprLogicalType::Int64)
            }
            ExprLogicalType::AnyInt => {
                // safety: this should always return Some because we can always determine the
                // logical arrow data type for integer types
                let arrow_data_type = left_int_type.datatype().expect("single data type");
                cast_expr(right_expr, arrow_data_type);
                *right_type = left_int_type.clone();
                Some(left_int_type.clone())
            }
            right_int_type if right_int_type.is_integer() => {
                if *left_int_type == *right_int_type {
                    // nothing to do, types already equal
                    Some(left_int_type.clone())
                } else {
                    // Coerce to the appropriate type based on signedness and bit width
                    let coerced_type = coerce_integer_types(left_int_type, right_int_type);

                    // Cast both sides to the coerced type
                    let target_datatype =
                        coerced_type.datatype().expect("integer type has datatype");
                    cast_expr(left_expr, target_datatype.clone());
                    *left_type = coerced_type.clone();

                    cast_expr(right_expr, target_datatype);
                    *right_type = coerced_type.clone();

                    Some(coerced_type)
                }
            }
            _ => {
                // other types can't be treated as integers
                None
            }
        },

        // other types cannot be used as argument to arithmetic
        _ => None,
    }
}

/// identifier of metric data point type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum MetricDataPointType {
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    SummaryDataPoint,
}

impl MetricDataPointType {
    /// Get the OTAP payload type containing data points of this metric type
    pub fn payload_type(&self) -> ArrowPayloadType {
        match self {
            Self::SummaryDataPoint => ArrowPayloadType::SummaryDataPoints,
            Self::ExponentialHistogramDataPoint => ArrowPayloadType::ExpHistogramDataPoints,
            Self::HistogramDataPoint => ArrowPayloadType::HistogramDataPoints,
            Self::NumberDataPoint => ArrowPayloadType::NumberDataPoints,
        }
    }

    /// return the [`ArrowPayloadType`] associated with data points of this data point type
    pub fn dp_attrs_payload_type(&self) -> ArrowPayloadType {
        match self {
            Self::SummaryDataPoint => ArrowPayloadType::SummaryDpAttrs,
            Self::ExponentialHistogramDataPoint => ArrowPayloadType::ExpHistogramDpAttrs,
            Self::HistogramDataPoint => ArrowPayloadType::HistogramDpAttrs,
            Self::NumberDataPoint => ArrowPayloadType::NumberDpAttrs,
        }
    }

    /// return the [`ArrowPayloadType`] associated with exemplars of this data point type
    pub fn exemplar_payload_type(&self) -> Option<ArrowPayloadType> {
        match self {
            Self::ExponentialHistogramDataPoint => Some(ArrowPayloadType::ExpHistogramDpExemplars),
            Self::HistogramDataPoint => Some(ArrowPayloadType::HistogramDpExemplars),
            Self::NumberDataPoint => Some(ArrowPayloadType::NumberDpExemplars),
            Self::SummaryDataPoint => None,
        }
    }

    /// return the [`ArrowPayloadType`] of attributes of exemplars of this data point type
    pub fn exemplar_attr_payload_type(&self) -> Option<ArrowPayloadType> {
        match self {
            Self::ExponentialHistogramDataPoint => {
                Some(ArrowPayloadType::ExpHistogramDpExemplarAttrs)
            }
            Self::HistogramDataPoint => Some(ArrowPayloadType::HistogramDpExemplarAttrs),
            Self::NumberDataPoint => Some(ArrowPayloadType::NumberDpExemplarAttrs),
            Self::SummaryDataPoint => None,
        }
    }

    /// returns an iterator of all the types of metric data points
    pub fn all() -> impl Iterator<Item = Self> {
        [
            MetricDataPointType::NumberDataPoint,
            MetricDataPointType::HistogramDataPoint,
            MetricDataPointType::ExponentialHistogramDataPoint,
            MetricDataPointType::SummaryDataPoint,
        ]
        .into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::logical_expr::Expr;

    /// Test helper pair: a logical expression and its type.
    struct TestExpr {
        logical_expr: Expr,
        expr_type: ExprLogicalType,
    }

    fn test_expr(expr_type: ExprLogicalType) -> TestExpr {
        TestExpr {
            expr_type,
            logical_expr: Expr::default(),
        }
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_any_value() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::AnyValue);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::AnyValueNumeric));
        assert_eq!(left_expr.expr_type, ExprLogicalType::AnyValueNumeric);
        assert_eq!(right_expr.expr_type, ExprLogicalType::AnyValueNumeric);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_float64() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::Float64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Float64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Float64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Float64);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_int64() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_scalar_int() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::AnyInt);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::AnyInt);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_int32() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::Int32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_uint32() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::UInt32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_right_string() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValue);
        let mut right_expr = test_expr(ExprLogicalType::String);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_arithmetic_left_scalar_int_right_int64() {
        let mut left_expr = test_expr(ExprLogicalType::AnyInt);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::AnyInt);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_scalar_int_right_any_value() {
        let mut left_expr = test_expr(ExprLogicalType::AnyInt);
        let mut right_expr = test_expr(ExprLogicalType::AnyValue);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::AnyInt);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_scalar_int_right_int32() {
        let mut left_expr = test_expr(ExprLogicalType::AnyInt);
        let mut right_expr = test_expr(ExprLogicalType::Int32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int32);
    }

    #[test]
    fn test_coerce_arithmetic_left_scalar_int_right_uint32() {
        let mut left_expr = test_expr(ExprLogicalType::AnyInt);
        let mut right_expr = test_expr(ExprLogicalType::UInt32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::UInt32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::UInt32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::UInt32);
    }

    #[test]
    fn test_coerce_arithmetic_left_scalar_int_right_float64() {
        let mut left_expr = test_expr(ExprLogicalType::AnyInt);
        let mut right_expr = test_expr(ExprLogicalType::Float64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_arithmetic_left_float64_right_float64() {
        let mut left_expr = test_expr(ExprLogicalType::Float64);
        let mut right_expr = test_expr(ExprLogicalType::Float64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Float64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Float64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Float64);
    }

    #[test]
    fn test_coerce_arithmetic_left_float64_right_any_value() {
        let mut left_expr = test_expr(ExprLogicalType::Float64);
        let mut right_expr = test_expr(ExprLogicalType::AnyValue);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Float64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Float64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Float64);
    }

    #[test]
    fn test_coerce_arithmetic_left_float64_right_int64() {
        let mut left_expr = test_expr(ExprLogicalType::Float64);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_arithmetic_left_int64_right_int64() {
        let mut left_expr = test_expr(ExprLogicalType::Int64);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_int64_right_any_value() {
        let mut left_expr = test_expr(ExprLogicalType::Int64);
        let mut right_expr = test_expr(ExprLogicalType::AnyValue);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_int64_right_scalar_int() {
        let mut left_expr = test_expr(ExprLogicalType::Int64);
        let mut right_expr = test_expr(ExprLogicalType::AnyInt);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_int64_right_int32() {
        let mut left_expr = test_expr(ExprLogicalType::Int64);
        let mut right_expr = test_expr(ExprLogicalType::Int32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_int32_right_int32() {
        let mut left_expr = test_expr(ExprLogicalType::Int32);
        let mut right_expr = test_expr(ExprLogicalType::Int32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int32);
    }

    #[test]
    fn test_coerce_arithmetic_left_int32_right_uint32() {
        let mut left_expr = test_expr(ExprLogicalType::Int32);
        let mut right_expr = test_expr(ExprLogicalType::UInt32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_int32_right_any_value() {
        let mut left_expr = test_expr(ExprLogicalType::Int32);
        let mut right_expr = test_expr(ExprLogicalType::AnyValue);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_left_int32_right_scalar_int() {
        let mut left_expr = test_expr(ExprLogicalType::Int32);
        let mut right_expr = test_expr(ExprLogicalType::AnyInt);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int32);
    }

    #[test]
    fn test_coerce_arithmetic_left_int32_right_float64() {
        let mut left_expr = test_expr(ExprLogicalType::Int32);
        let mut right_expr = test_expr(ExprLogicalType::Float64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_arithmetic_left_uint32_right_uint32() {
        let mut left_expr = test_expr(ExprLogicalType::UInt32);
        let mut right_expr = test_expr(ExprLogicalType::UInt32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::UInt32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::UInt32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::UInt32);
    }

    #[test]
    fn test_coerce_arithmetic_left_uint8_right_uint8() {
        let mut left_expr = test_expr(ExprLogicalType::UInt8);
        let mut right_expr = test_expr(ExprLogicalType::UInt8);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::UInt8));
        assert_eq!(left_expr.expr_type, ExprLogicalType::UInt8);
        assert_eq!(right_expr.expr_type, ExprLogicalType::UInt8);
    }

    #[test]
    fn test_coerce_arithmetic_left_string_right_string() {
        let mut left_expr = test_expr(ExprLogicalType::String);
        let mut right_expr = test_expr(ExprLogicalType::String);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_arithmetic_left_boolean_right_boolean() {
        let mut left_expr = test_expr(ExprLogicalType::Boolean);
        let mut right_expr = test_expr(ExprLogicalType::Boolean);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_numeric_right_any_value_numeric() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValueNumeric);
        let mut right_expr = test_expr(ExprLogicalType::AnyValueNumeric);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::AnyValueNumeric));
        assert_eq!(left_expr.expr_type, ExprLogicalType::AnyValueNumeric);
        assert_eq!(right_expr.expr_type, ExprLogicalType::AnyValueNumeric);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_numeric_right_float64() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValueNumeric);
        let mut right_expr = test_expr(ExprLogicalType::Float64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Float64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Float64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Float64);
    }

    #[test]
    fn test_coerce_arithmetic_left_any_value_numeric_right_int64() {
        let mut left_expr = test_expr(ExprLogicalType::AnyValueNumeric);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    // Tests for mixed integer type coercion

    #[test]
    fn test_coerce_arithmetic_uint8_plus_uint32() {
        // Both unsigned, coerce to larger width (UInt32)
        let mut left_expr = test_expr(ExprLogicalType::UInt8);
        let mut right_expr = test_expr(ExprLogicalType::UInt32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::UInt32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::UInt32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::UInt32);
    }

    #[test]
    fn test_coerce_arithmetic_uint8_plus_int32() {
        // Unsigned + signed, coerce to signed with same width (Int32)
        let mut left_expr = test_expr(ExprLogicalType::UInt8);
        let mut right_expr = test_expr(ExprLogicalType::Int32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int32));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int32);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int32);
    }

    #[test]
    fn test_coerce_arithmetic_uint32_plus_int32() {
        // Unsigned + signed with same width, need to upsize to avoid overflow (Int64)
        let mut left_expr = test_expr(ExprLogicalType::UInt32);
        let mut right_expr = test_expr(ExprLogicalType::Int32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_int32_plus_uint32() {
        // Signed + unsigned with same width (reverse order), should give same result
        let mut left_expr = test_expr(ExprLogicalType::Int32);
        let mut right_expr = test_expr(ExprLogicalType::UInt32);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_uint8_plus_int64() {
        // Small unsigned + large signed, coerce to larger signed (Int64)
        let mut left_expr = test_expr(ExprLogicalType::UInt8);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }

    #[test]
    fn test_coerce_arithmetic_uint32_plus_int64() {
        // Unsigned 32 + signed 64, coerce to larger signed (Int64)
        let mut left_expr = test_expr(ExprLogicalType::UInt32);
        let mut right_expr = test_expr(ExprLogicalType::Int64);
        let result = coerce_arithmetic(
            &mut left_expr.logical_expr,
            &mut left_expr.expr_type,
            &mut right_expr.logical_expr,
            &mut right_expr.expr_type,
        );
        assert_eq!(result, Some(ExprLogicalType::Int64));
        assert_eq!(left_expr.expr_type, ExprLogicalType::Int64);
        assert_eq!(right_expr.expr_type, ExprLogicalType::Int64);
    }
}
