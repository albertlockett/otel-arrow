// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module contains some custom datafusion plan steps & optimizer rules for filtering
//! OTAP batches by attributes

use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, ready};

use arrow::array::{BooleanBuilder, RecordBatch, StringArray, UInt16Array};
use arrow::compute::kernels::cmp::eq;
use arrow::compute::{and, filter, filter_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::print_batches;
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    InvariantLevel, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use futures_core::Stream;
use futures_util::StreamExt;
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_rust::schema::consts;
use roaring::RoaringBitmap;

pub struct AttributeFilterExpr {
    // TODO maybe not public
    // TODO maybe better naming
    // TODO this is only here so we can drill it through to the first instance of the exec stream
    pub attrs_batch: RecordBatch,
}

/// Implementation of extension for DataFusion's Logical Plan that specifies an attribute filter
#[derive(Debug)]
pub struct AttributeFilterExtension {
    // TODO keys and values expressions
    // key: String,
    // // TODO this should be some kind of anyvalue type
    // value: String,
    input: LogicalPlan,

    attrs_batch: RecordBatch,
}

// TODO not sure about these implementations of PartialEq, Eq, Hash and PartialOrd
// need to figure out why these are required by the USerDefinedLogicalNodeCore trait

impl PartialEq for AttributeFilterExtension {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}

impl Eq for AttributeFilterExtension {}

impl Hash for AttributeFilterExtension {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl PartialOrd for AttributeFilterExtension {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl AttributeFilterExtension {
    pub fn new(input: LogicalPlan, attrs_batch: RecordBatch) -> Self {
        Self { input, attrs_batch }
    }
}

impl UserDefinedLogicalNodeCore for AttributeFilterExtension {
    fn name(&self) -> &str {
        "OtapAttributeFilter"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        // TODO
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        // TODO
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        // TODO
        write!(f, "TODO")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            // TODO return an error here
            panic!("unexpected input")
        }

        // TODO avoid clone?
        Ok(Self {
            input: inputs[0].clone(),
            attrs_batch: self.attrs_batch.clone(),
        })
    }
}

#[derive(Debug)]
pub struct AttributeFilterExec {
    pub source: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,
    pub arrow_payload_type: ArrowPayloadType,
    pub curr_batch: RwLock<RecordBatch>,
}

impl AttributeFilterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, batch: RecordBatch) -> Self {
        let schema = input.schema();
        Self {
            source: input,
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),

            // TODO not have this hard-coded to logs
            arrow_payload_type: ArrowPayloadType::LogAttrs,
            curr_batch: RwLock::new(batch),
        }
    }

    pub fn update_batch(&self, next_batch: RecordBatch) {
        let mut guard = self.curr_batch.write().expect("mutex poisoned");
        *guard = next_batch
    }
}

// TODO should just use the DefaultDisplayAs for this?
impl DisplayAs for AttributeFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AttributeFilterExec: ")?;
            }
            DisplayFormatType::TreeRender => {}
        }
        self.source.fmt_as(t, f)
    }
}

impl ExecutionPlan for AttributeFilterExec {
    fn name(&self) -> &str {
        "OtapAttributeFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            // TODO return an error
            panic!("wrong children length")
        }

        let batch = {
            let guard = self.curr_batch.read().expect("mutex poisoned");
            (*guard).clone()
        };

        let input = children[0].clone();
        Ok(Arc::new(Self::new(input, batch)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let ids_col = {
            let guard = self.curr_batch.read().expect("mutex poisoned");
            let curr_batch = guard;

            // print_batches(&[curr_batch.clone()]).unwrap();
            let key_col = curr_batch.column_by_name(consts::ATTRIBUTE_KEY).unwrap();
            let key_mask = eq(key_col, &StringArray::new_scalar("k8s.ns")).unwrap();
            let val_col = curr_batch.column_by_name(consts::ATTRIBUTE_STR).unwrap();
            let val_mask = eq(val_col, &StringArray::new_scalar("prod")).unwrap();

            let mask = and(&key_mask, &val_mask).unwrap();

            let parent_ids = curr_batch.column_by_name(consts::PARENT_ID).unwrap();
            filter(&parent_ids, &mask).unwrap()
        };

        let u16_ids = ids_col.as_any().downcast_ref::<UInt16Array>().unwrap();

        let id_bitmap: RoaringBitmap = u16_ids.iter().flatten().map(|i| i as u32).collect();

        // let mut id_bitmap = RoaringBitmap::new();
        // id_bitmap.insert(0);
        // id_bitmap.insert(2);

        let input_stream = self.source.execute(partition, context)?;
        let schema = input_stream.schema();

        Ok(Box::pin(AttributeFilterExecStream {
            input: input_stream,
            id_bitmap,
            schema,
        }))
    }
}

pub struct AttributeFilterExecStream {
    input: SendableRecordBatchStream,
    id_bitmap: RoaringBitmap,
    schema: SchemaRef,
}

fn batch_filter(batch: &RecordBatch, id_bitmap: &RoaringBitmap) -> Result<RecordBatch> {
    // TODO handle case where this ID column isn't present or is some other type
    let id_column = batch
        .column_by_name(consts::ID)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    let mut mask_builder = BooleanBuilder::with_capacity(id_column.len());

    for id in id_column {
        let is_present = match id {
            Some(id) => id_bitmap.contains(id as u32),
            None => false,
        };
        mask_builder.append_value(is_present);
    }

    let mask = mask_builder.finish();
    let filtered_batch = filter_record_batch(batch, &mask).unwrap();

    Ok(filtered_batch)
}

impl Stream for AttributeFilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let filtered_batch = batch_filter(&batch, &self.id_bitmap)?;
                    if filtered_batch.num_rows() == 0 {
                        continue;
                    }

                    poll = Poll::Ready(Some(Ok(filtered_batch)));
                    break;
                }
                value => {
                    poll = Poll::Ready(value);
                    break;
                }
            }
        }

        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for AttributeFilterExecStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub struct AttributeFilterExecPlanner {}

impl AttributeFilterExecPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for AttributeFilterExecPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(attr_filter) = node.as_any().downcast_ref::<AttributeFilterExtension>() {
                // TODO prob don't wanna panic here
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                Some(Arc::new(AttributeFilterExec::new(
                    physical_inputs[0].clone(),
                    attr_filter.attrs_batch.clone(),
                )))
            } else {
                None
            },
        )
    }
}
