// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Utilities for evaluating [`ScopedExpr`] on an individual record batch.
//!
//! This type of execution would most appropriately be used during evaluation of nested pipelines
//! applied to attributes such as in the OPL query `logs | apply attributes { where <expr> }`.
//! In this case, we'd evaluate the expression (which would be planned as a datafusion expression)
//! on the attributes record batch containing the attributes.
//!
//! Evaluation on attributes [`RecordBatch`]s presents a few interesting challenges..
//!
//! For one thing the values columns are all optional (refer to OTAP spec), which means that
//! a) a given column used in some expression may be absent and
//! b) the column order may change during from one batch to the next.
//! This is not unique to attributes, but it must be dealt with.
//!
//! The second challenge, which is unique to attributes, is that we allow expressions to be
//! planned referencing a virtual `value` column, which must be resolved at execution time.
//! In some cases, especially in the case of logical expressions, we may be able to infer it,
//! for example in expressions like `value > 2` we know the target column is `int`. In other
//! cases, the resolution is impossible, for example in cases like `set value = value + value`
//! (we know it may be either integer or double). In these cases, we must partition the batch
//! by type and evaluate for all possible value types and stitch the result back together.
//!

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, NullArray,
    RecordBatch, StringArray, UInt8Array,
};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::kernels::cmp::neq;
use arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::empty;
use datafusion::scalar::ScalarValue;
use otel_arrow_dfe_pdata::error::Error as PdataError;
use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;
use otel_arrow_dfe_pdata::schema::consts;

use crate::error::{Error, Result};
use crate::pipeline::expr::VALUE_COLUMN_NAME;
use crate::pipeline::expr::eval::evaluate_df_expr;
use crate::pipeline::expr::{LeafEval, eval::EvalContext};
use crate::pipeline::project::{Projection, ProjectionColumns, ProjectionOptions};

// TODO - is this module special for attributes?

/// Evaluate this node directly on the provided `RecordBatch`, ignoring scope resolution.
///
/// This expects the leaf_eval to be the `DatafusionExpr` variant, and if this variant is
/// not found then an error is returned.
///
pub(crate) fn evaluate_on_attrs_batch(
    attrs_record_batch: &RecordBatch,
    leaf_eval: &mut LeafEval,
    eval_ctx: &EvalContext<'_>,
) -> Result<ColumnarValue> {
    match leaf_eval {
        LeafEval::DatafusionExpr {
            logical_expr,
            physical_expr,
            projection,
            projection_opts,
            missing_data_passes,
            ..
        } => {
            match try_project_attrs_record_batch(attrs_record_batch, projection, projection_opts)? {
                Some(projected_rb) => {
                    evaluate_df_expr(&logical_expr, physical_expr, eval_ctx, &projected_rb)
                }
                None => {
                    if *missing_data_passes {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                    } else {
                        Ok(ColumnarValue::Scalar(ScalarValue::Null))
                    }
                }
            }
        }
        _ => Err(Error::InvalidPipelineError {
            cause: "only Eval(DatafusionExpr) can be evaluated on a provided batch".into(),
            query_location: None,
        }),
    }
}

// TODO - this assumes a homogenously non-null column for some type.  we need to test
// how null values can actually be handled.
fn try_project_attrs_record_batch(
    attrs_record_batch: &RecordBatch,
    projection: &Projection,
    projection_opts: &ProjectionOptions,
) -> Result<Option<RecordBatch>> {
    let mut projection_cols = ProjectionColumns::from(attrs_record_batch);

    // TODO boolean logic is hard
    // TODO the whole body is a big IF
    // TODO early return for empty?
    let empty_projection = projection.schema().len() == 0;
    let keys_only =
        projection.schema().len() == 1 && projection.references_column(consts::ATTRIBUTE_KEY);
    if !empty_projection && !keys_only {
        let input_schema = attrs_record_batch.schema_ref();
        let (type_col_index, _) = input_schema
            .fields
            .find(consts::ATTRIBUTE_TYPE)
            .ok_or_else(|| PdataError::ColumnNotFound {
                name: consts::ATTRIBUTE_TYPE.into(),
            })?;

        let type_column = attrs_record_batch.column(type_col_index);
        let type_column = type_column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| Error::ExecutionError {
                cause: PdataError::ColumnDataTypeMismatch {
                    name: consts::ATTRIBUTE_TYPE.into(),
                    expect: DataType::UInt8,
                    actual: type_column.data_type().clone(),
                }
                .to_string(),
            })?;

        if type_column.null_count() != 0 {
            // even though we only look at the first non-null value to determine the input type,
            // we'll be strict here validate that there aren't any nulls
            return Err(Error::ExecutionError {
                cause: "attribute record batch type column should not contain nulls".into(),
            });
        }

        // TODO -- we actually haven't checked this I don't think now that we've refactored things.
        // safety: we've already checked the batch is not empty, and that there aren't any nulls
        // in this column, which means we should be safe to expect at least one non-null type
        let input_attr_type = type_column
            .iter()
            .flatten()
            .next()
            .expect("non-empty batch");

        let input_attr_type =
            AttributeValueType::try_from(input_attr_type).map_err(|e| Error::ExecutionError {
                cause: format!("invalid attribute type {input_attr_type}: {e}"),
            })?;

        // check if every value is the same type - if not, we may have problems evaluating the
        // expression (if the value is used in the expression).
        let all_rows_same_attr_type =
            neq(type_column, &UInt8Array::new_scalar(input_attr_type as u8))?.true_count() == 0;

        if !all_rows_same_attr_type {
            // if not all the attribute types are the same, we can't determine a single value
            // column to use in the projection, so return an error for now. In practice, the batch
            // should be split apart before this pipeline stage using other operators to ensure
            // we only have one value type; for example:
            // - `if (value is Integer) { ... }`
            // - `value as Integer` (explicit cast)
            // - `if (key == "something") { ... }` which may assume that all values for some key have
            //   a homogenous type.
            //
            // In some rare cases, it may be possible to write an expression that makes sense on
            // multiple types simultaneously .. e.g. things like `value + value` could be an int
            // or a double. For now, we'll force the user to handle this explicitly.
            return Err(Error::ExecutionError {
                cause: "All input rows for attribute assignment must have the same type \
                            if value used in expression"
                    .into(),
            });
        }

        // get the name of the column containing the actual values identified by the type column.
        // this is named "physical_..." to distinguish it from the virtual "values" column which
        // might also be found in the projection
        let physical_values_column_name = match input_attr_type {
            AttributeValueType::Bool => Some(consts::ATTRIBUTE_BOOL),
            AttributeValueType::Double => Some(consts::ATTRIBUTE_DOUBLE),
            AttributeValueType::Int => Some(consts::ATTRIBUTE_INT),
            AttributeValueType::Str => Some(consts::ATTRIBUTE_STR),
            AttributeValueType::Empty => None,
            other => {
                return Err(Error::NotYetSupportedError {
                    message: format!(
                        "Setting attributes of type {:?} in nested pipeline not yet supported",
                        other
                    ),
                });
            }
        };

        let num_rows = attrs_record_batch.num_rows();

        // insert default placeholder column for column values only if projection references it
        if let Some(values_column_name) = physical_values_column_name {
            let existing_col = projection_cols.find(values_column_name);
            let projection_references_values_col = projection.references_column(values_column_name);

            if projection_references_values_col && existing_col.is_none() {
                let values_col = default_values_column(input_attr_type, num_rows);
                projection_cols.append_column(
                    Field::new(values_column_name, values_col.data_type().clone(), true),
                    values_col,
                );
            }
        }

        // create the virtual "values" column
        if projection.references_values_column() {
            if let Some(values_column_name) = physical_values_column_name {
                if let Some((index, field)) = projection_cols.find(values_column_name) {
                    if projection.schema().len() > 0
                        && projection.references_column(values_column_name)
                    {
                        // need to duplicate the column to be the values column
                        projection_cols.append_column(
                            field.as_ref().clone().with_name(VALUE_COLUMN_NAME),
                            projection_cols.columns()[index].clone(),
                        );
                    } else {
                        projection_cols.rename_column_at_index(index, VALUE_COLUMN_NAME);
                    }
                } else {
                    // need to insert placeholder column with the correct attributes type full of
                    // default values
                    let values_col = default_values_column(input_attr_type, num_rows);
                    projection_cols.append_column(
                        Field::new(VALUE_COLUMN_NAME, values_col.data_type().clone(), true),
                        values_col,
                    );
                }
            } else {
                // "Empty" type attribute doesn't have a values column, so create a new virtual
                // all-null column
                projection_cols.append_column(
                    Field::new(VALUE_COLUMN_NAME, DataType::Null, true),
                    Arc::new(NullArray::new(num_rows)),
                );
            }
        }
    }

    projection.project_with_options(projection_cols, projection_opts)
}

fn default_values_column(attr_type: AttributeValueType, num_rows: usize) -> ArrayRef {
    match attr_type {
        AttributeValueType::Bool => {
            Arc::new(BooleanArray::new(BooleanBuffer::new_unset(num_rows), None))
        }
        AttributeValueType::Double => Arc::new(Float64Array::new(
            ScalarBuffer::from(vec![0.0; num_rows]),
            None,
        )),
        AttributeValueType::Int => Arc::new(DictionaryArray::new(
            UInt8Array::new(ScalarBuffer::from(vec![0u8; num_rows]), None),
            Arc::new(Int64Array::new(ScalarBuffer::from(vec![0]), None)),
        )),
        AttributeValueType::Str => Arc::new(DictionaryArray::new(
            UInt8Array::new(ScalarBuffer::from(vec![0u8; num_rows]), None),
            Arc::new(StringArray::new(
                OffsetBuffer::new_zeroed(num_rows),
                MutableBuffer::new(0).into(),
                None,
            )),
        )),
        _ => {
            todo!("I _think_ this is unreachable")
        }
    }
}
