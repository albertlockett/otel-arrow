use std::fs::File;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use arrow::util::pretty::print_batches;
use datafusion::catalog::MemTable;
use datafusion::common::JoinType;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{ParquetReadOptions, col, lit};

// some use cases we want to cover:
//
// traces:
//     span:
//     - 'attributes["container.name"] == "app_container_1"'
//     - 'resource.attributes["host.name"] == "localhost"'
//     - 'name == "app_3"'
//     spanevent:
//     - 'attributes["grpc"] == true'
//     - 'IsMatch(name, ".*grpc.*")'
// metrics:
//     metric:
//         - 'name == "my.metric" and resource.attributes["my_label"] == "abc123"'
//         - 'type == METRIC_DATA_TYPE_HISTOGRAM'
//     datapoint:
//         - 'metric.type == METRIC_DATA_TYPE_SUMMARY'
//         - 'resource.attributes["service.name"] == "my_service_name"'
// logs:
//     log_record:
//     - 'IsMatch(body, ".*password.*")'
//     - 'severity_number < SEVERITY_NUMBER_WARN'

fn read_record_batches(file_path: &str) -> RecordBatch {
    let file = File::open(file_path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = concat_batches(batches[0].schema_ref(), &batches).unwrap();

    batch
}

#[tokio::main]
async fn main() {
    let logs = read_record_batches("/tmp/logs.arrow");
    let log_attrs = read_record_batches("/tmp/logattrs.arrow");

    let ctx = SessionContext::new();
    let logs_table = MemTable::try_new(logs.schema(), vec![vec![logs]]).unwrap();
    let log_attrs_table = MemTable::try_new(log_attrs.schema(), vec![vec![log_attrs]]).unwrap();
    ctx.register_table("logs", Arc::new(logs_table)).unwrap();
    ctx.register_table("log_attrs", Arc::new(log_attrs_table))
        .unwrap();

    print_batches(&ctx.sql("select * from log_attrs offset 20 limit 30").await.unwrap().collect().await.unwrap()).unwrap();

    // filter example 1:
    // logs:
    //   log_record:
    //   - attributes["gen_ai.system"] == "openai"
    //   - attributes["session.id"] != nil
    //   - attributes["azure.service.request.id"] == nil

    let gen_ai_filter = col("key").eq(lit("gen_ai.system")).and(col("str").eq(lit("openai")));
    let has_session_id = col("key").eq(lit("session.id"));
    let doesnt_have_azure_service_request_id = col("key").eq(lit("azure.service.request.id"));


    let attrs_anti_filter = ctx
        .table("log_attrs")
        .await
        .unwrap()
        .filter(gen_ai_filter.or(has_session_id))
        .unwrap();

    let attrs_filter = ctx
        .table("log_attrs")
        .await
        .unwrap()
        .filter(doesnt_have_azure_service_request_id)
        .unwrap();

    let logs = ctx
        .table("logs")
        .await
        .unwrap()
        .join(
            attrs_anti_filter,
            JoinType::LeftAnti,
            &["id"],
            &["parent_id"],
            None,
        )
        .unwrap()
        .join(
            attrs_filter,
            JoinType::Inner,
            &["id"],
            &["parent_id"],
            None,
        )
        .unwrap()
        .sort_by(vec![col("id")])
        .unwrap();

    let batches = logs.collect().await.unwrap();
    print_batches(&[batches[0].slice(0, 10)]).unwrap();
}
