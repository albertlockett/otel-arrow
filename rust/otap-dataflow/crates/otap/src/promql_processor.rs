
//! TODO


#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};
    use arrow::util::pretty::print_batches;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::execute_stream;
    use datafusion::{
        catalog::CatalogProviderList,
        logical_expr::LogicalPlan,
    };
    use greptime_base_common::plugins::Plugins;
    use greptimedb_catalog::memory::MemoryCatalogManager;
    use greptimedb_catalog::table_source::DfTableSourceProvider;
    use greptimedb_catalog::{RegisterSchemaRequest, RegisterTableRequest};
    use greptimedb_datatypes::schema::TIME_INDEX_KEY;
    use greptimedb_datatypes::vectors::Helper;
    use greptimedb_query::plan::TableType;
    use greptimedb_query::promql::planner::PromPlanner;
    use greptimedb_query::options::QueryOptions;
    use greptimedb_query::query_engine::QueryEngineState;
    use greptimedb_query_common::{
        error::Result as GreptimeQueryCommonResult,
        logical_plan::SubstraitPlanDecoder,
    };
    use greptimedb_session::context::QueryContext;
    use greptimedb_table::metadata::{TableIdent, TableInfo, TableInfoBuilder, TableMeta, TableMetaBuilder};
    use greptimedb_table::test_util::memtable::MemTable;
    use greptimedb_table::Table;
    use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, InstrumentationScope};
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{AggregationTemporality, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum};
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::proto::OtlpProtoMessage;
    use otap_df_pdata::schema::consts;
    use otap_df_pdata::testing::round_trip::otlp_to_otap;
    use promql_parser::parser::{parse, EvalStmt};

    pub struct DummyDecoder;

    impl DummyDecoder {
        pub fn arc() -> Arc<Self> {
            Arc::new(DummyDecoder)
        }
    }

    #[async_trait::async_trait]
    impl SubstraitPlanDecoder for DummyDecoder {
        async fn decode(
            &self,
            _message: bytes::Bytes,
            _catalog_list: Arc<dyn CatalogProviderList>,
            _optimize: bool,
        ) -> GreptimeQueryCommonResult<LogicalPlan> {
            unreachable!()
        }
    }


    fn gen_metrics() -> MetricsData {
        MetricsData::new(vec![
            ResourceMetrics::new(
                Resource::default(),
                vec![
                    ScopeMetrics::new(InstrumentationScope::default(), vec![
                        Metric::build()
                            .name("albert_metric")
                            .data_sum(Sum {
                                data_points: vec![
                                    NumberDataPoint::build()
                                        .value_int(1)
                                        .time_unix_nano(1u64)
                                        .attributes(vec![
                                            KeyValue::new("environment", AnyValue::new_string("prod")),
                                        ])
                                        .finish(),
                                    NumberDataPoint::build()
                                        .value_int(1)
                                        .time_unix_nano(1u64)
                                        .attributes(vec![
                                            KeyValue::new("environment", AnyValue::new_string("staging")),
                                        ])
                                        .finish()
                                ],
                                aggregation_temporality: AggregationTemporality::Cumulative.into(),
                                is_monotonic: true,
                            })
                            .finish()
                            
                    ])
                ]
            )
        ])
    }

    #[tokio::test]
    async fn test_otap_metrics_exec() {
        let promql = r#"albert_metric{}"#;
        let expr = parse(promql).unwrap();
        let query_context = Arc::new(QueryContext::with("albertcat", "albertschema"));
        
        let manager = MemoryCatalogManager::new();
        let _ = manager.register_catalog_sync("albertcat");
        let _ = manager.register_schema_sync(RegisterSchemaRequest {
            catalog: "albertcat".into(),
            schema: "albertschema".into()
        });

        let metrics_data = gen_metrics();
        let otap_batch = otlp_to_otap(&OtlpProtoMessage::Metrics(metrics_data));
        let rb = otap_batch.get(ArrowPayloadType::NumberDataPoints).unwrap().clone();

        // put the timestamp column
        let new_fields = rb.schema().fields().clone().iter().map(|field| {
            if field.name() == consts::TIME_UNIX_NANO {
                field.as_ref().clone().with_metadata(HashMap::from_iter([
                    (TIME_INDEX_KEY.to_string().clone(), "true".into())
                ]))
            } else {
                field.as_ref().clone()
            }
        }).collect::<Vec<_>>();

        let arrow_schema: Arc<arrow::datatypes::Schema> = Arc::new(arrow::datatypes::Schema::new(new_fields));
        let gdb_schema = arrow_schema.try_into().unwrap();
        // gdb_schema.
        let grb = greptime_recordbatch_common::recordbatch::RecordBatch::new(
                Arc::new(gdb_schema),
                rb.columns().into_iter().map(|v| {
                    Helper::try_into_vector(v).unwrap()
                })
            ).unwrap();
        let memtable = MemTable::new_with_catalog(
            "albert_metric",
            grb,
            0, 
            "albertcat".into(), 
            "albertschema".into(),
            vec![]
        );


        let _ = manager.register_table_sync(RegisterTableRequest { 
            catalog: "albertcat".into(), 
            schema: "albertschema".into(), 
            table_name: "albert_metric".into(), 
            table_id: 0, 
            table: memtable
        });

        let table_provider = DfTableSourceProvider::new(
            manager.clone(),
            true,
            query_context,
            DummyDecoder::arc(),
            true
        );

        let eval_stmr = EvalStmt {
            expr: expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let query_engine_state = QueryEngineState::new(
            manager.clone(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions::default()
        );

        let logical_plan = PromPlanner::stmt_to_plan_with_alias(
            table_provider, 
            &eval_stmr, 
            None, 
            &query_engine_state,
        ).await.unwrap();

        println!("logical plan = {}", logical_plan);

        let session_state = query_engine_state.session_state();
        let physical_plan = session_state.create_physical_plan(&logical_plan).await.unwrap();
        let task_context = Arc::new(TaskContext::from(&session_state));
        let stream = execute_stream(physical_plan, task_context).unwrap();
        let batches = collect(stream).await.unwrap();
        print_batches(&batches).unwrap();        
    }
}