use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::util::pretty::print_batches;
use data_engine_expressions::{
    DataExpression, DiscardDataExpression, EqualToLogicalExpression, IntegerValue,
    LogicalExpression, NotLogicalExpression, PipelineExpression, ScalarExpression,
    SourceScalarExpression, StaticScalarExpression, StringValue,
};
use data_engine_kql_parser::{KqlParser, Parser};
use datafusion::common::Column;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{self, BinaryExpr, Expr, LogicalPlanBuilder, Operator};
use datafusion::prelude::ParquetReadOptions;
use datafusion::scalar::ScalarValue;

#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();

    let parquet_reader_opts =
        ParquetReadOptions::new().table_partition_cols(vec![("_part_id".into(), DataType::Utf8)]);

    ctx.register_parquet("logs", "/tmp/logs", parquet_reader_opts.clone())
        .await
        .unwrap();

    ctx.register_parquet("log_attrs", "/tmp/log_attrs", parquet_reader_opts.clone())
        .await
        .unwrap();

    // example kql queries for log_attrs:
    // let kql_query = "source | where key == \"error.type\"";
    // let kql_query = "source | where key != \"error.type\"";
    // let kql_query = "source | where type >= 2";
    // let kql_query = "source | where type < 2";
    let kql_query = "source | where key has \"error\"";

    let pipeline_expr = KqlParser::parse(kql_query).unwrap();
    println!("pipeline expr = {:#?}", pipeline_expr);

    let table = ctx.table("log_attrs").await.unwrap();
    let plan_builder = LogicalPlanBuilder::new(table.logical_plan().clone());
    let logical_plan = transform_df_plan(plan_builder, &pipeline_expr)
        .build()
        .unwrap();

    let df = ctx
        .execute_logical_plan(logical_plan.clone())
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    // Print results in a nice table
    println!("got {} batches ...", batches.len());

    // pretty print the result:
    if !batches.is_empty() {
        let schema = batches[0].schema();
        let result = concat_batches(&schema, batches.iter()).unwrap();
        print_batches(&[result]).unwrap();
    }
}

fn transform_df_plan(
    mut df_logical_plan: LogicalPlanBuilder,
    pipeline_plan: &PipelineExpression,
) -> LogicalPlanBuilder {
    for data_expr in pipeline_plan.get_expressions() {
        df_logical_plan = transform_df_plan_data_expr(df_logical_plan, data_expr)
    }

    df_logical_plan
}

fn transform_df_plan_data_expr(
    df_logical_plan: LogicalPlanBuilder,
    data_expr: &DataExpression,
) -> LogicalPlanBuilder {
    match data_expr {
        DataExpression::Discard(discard_expr) => {
            transform_df_plan_discard_exp(df_logical_plan, discard_expr)
        }
        _ => {
            todo!()
        }
    }
}

fn transform_df_plan_discard_exp(
    df_logical_plan: LogicalPlanBuilder,
    discard_expr: &DiscardDataExpression,
) -> LogicalPlanBuilder {
    if let Some(logical_expr) = discard_expr.get_predicate() {
        let filter_expr = match logical_expr {
            // TODO -- why does "source | where ..." get parsed to Not?
            // for now we're just ignoring this `Not`
            LogicalExpression::Not(inner) => to_df_logical_expr(inner.get_inner_expression()),

            // otherwise just convert it like normal
            le => to_df_logical_expr(le),
        };

        df_logical_plan.filter(filter_expr).unwrap()
    } else {
        // nothing to do
        df_logical_plan
    }
}

fn to_df_logical_expr(logical_expr: &LogicalExpression) -> Expr {
    match logical_expr {
        LogicalExpression::Not(not_expr) => Expr::Not(Box::new(to_df_logical_expr(
            not_expr.get_inner_expression(),
        ))),
        LogicalExpression::GreaterThan(gt_expr) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_binary_expr_arg(gt_expr.get_left())),
            op: Operator::Gt,
            right: Box::new(to_binary_expr_arg(gt_expr.get_right())),
        }),
        LogicalExpression::GreaterThanOrEqualTo(gt_expr) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_binary_expr_arg(gt_expr.get_left())),
            op: Operator::GtEq,
            right: Box::new(to_binary_expr_arg(gt_expr.get_right())),
        }),
        LogicalExpression::EqualTo(eq_expr) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_binary_expr_arg(eq_expr.get_left())),
            op: Operator::Eq,
            right: Box::new(to_binary_expr_arg(eq_expr.get_right())),
        }),
        LogicalExpression::Contains(contains_expr) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_binary_expr_arg(contains_expr.get_haystack())),
            op: Operator::ILikeMatch,
            right: Box::new(contains_needle_to_ilike(contains_expr.get_needle())),
        }),
        _ => {
            todo!()
        }
    }
}

fn to_binary_expr_arg(scalar_expr: &ScalarExpression) -> Expr {
    match scalar_expr {
        ScalarExpression::Source(scalar_src) => to_df_column_expr(scalar_src),
        ScalarExpression::Static(scalar_static) => {
            let scalar_value = match scalar_static {
                StaticScalarExpression::String(static_str) => {
                    ScalarValue::Utf8(Some(static_str.get_value().to_string()))
                }
                StaticScalarExpression::Integer(static_int) => {
                    ScalarValue::Int64(Some(static_int.get_value()))
                }
                _ => {
                    todo!()
                }
            };

            Expr::Literal(scalar_value, None)
        }
        _ => {
            todo!()
        }
    }
}

fn contains_needle_to_ilike(scalar_expr: &ScalarExpression) -> Expr {
    match scalar_expr {
        ScalarExpression::Static(scalar_static) => {
            let scalar_value = match scalar_static {
                StaticScalarExpression::String(static_str) => {
                    ScalarValue::Utf8(Some(format!("%{}%", static_str.get_value().to_string())))
                }
                _ => {
                    todo!()
                }
            };

            Expr::Literal(scalar_value, None)
        }
        _ => {
            todo!()
        }
    }
}

fn to_df_column_expr(source_scalar: &SourceScalarExpression) -> Expr {
    let value_accessor = source_scalar.get_value_accessor();
    let selectors = value_accessor.get_selectors();
    let selector = &selectors[0];
    match selector {
        ScalarExpression::Static(StaticScalarExpression::String(string_scalar)) => {
            Expr::Column(Column::from_name(string_scalar.get_value()))
        }
        _ => {
            todo!()
        }
    }
}
