// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module contains some custom datafusion plan steps & optimizer rules for filtering
//! OTAP batches by attributes
//!
//! possibly evil things are happening in this module.
//
// TODO remove prior comment if these forbidden experiments turn out to be
// less evil than intended

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    InvariantLevel, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;

pub struct AttributeFilterExpr {}

/// Implementation of extension for DataFusion's Logical Plan that specifies an attribute filter
#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub struct AttributeFilterExtension {
    // TODO keys and values expressions
    // key: String,
    // // TODO this should be some kind of anyvalue type
    // value: String,
    input: LogicalPlan,
}

impl AttributeFilterExtension {
    pub fn new(input: LogicalPlan) -> Self {
        Self { input }
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
        })
    }
}

#[derive(Debug)]
pub struct AttributeFilterExec {
    source: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,
}

impl AttributeFilterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();
        Self {
            source: input,
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
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

        let input = children[0].clone();
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        println!("I executed");
        self.source.execute(partition, context)
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
            if let Some(_attr_filter) = node.as_any().downcast_ref::<AttributeFilterExtension>() {
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                Some(Arc::new(AttributeFilterExec::new(
                    physical_inputs[0].clone(),
                )))
            } else {
                None
            },
        )
    }
}
