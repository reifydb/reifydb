// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::WindowKind, interface::catalog::flow::OperatorId};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, value::duration::Duration};

use crate::{
	expression::Expression,
	flow::{
		aggregate::AggregateContext,
		compiler::{CompileOperator, FlowCompiler, operator::aggregate_validation::validate_flow_aggregations},
		operator::OperatorDef::Window,
	},
	nodes::WindowNode,
	query::QueryPlan,
};

pub(crate) struct WindowCompiler {
	pub input: Option<Box<QueryPlan>>,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub lateness: Duration,
	pub immutable: Option<Duration>,
}

impl From<WindowNode> for WindowCompiler {
	fn from(node: WindowNode) -> Self {
		Self {
			input: node.input,
			kind: node.kind,
			group_by: node.group_by,
			aggregations: node.aggregations,
			lateness: node.lateness,
			immutable: node.immutable,
		}
	}
}

impl CompileOperator for WindowCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		validate_flow_aggregations(&compiler.routines, &self.aggregations, AggregateContext::Windowed)?;

		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Window {
				kind: self.kind,
				group_by: self.group_by,
				aggregations: self.aggregations,
				lateness: self.lateness,
				immutable: self.immutable,
			},
		)?;

		if let Some(input_node) = input_node {
			compiler.add_edge(txn, &input_node, &node_id)?;
		}

		Ok(node_id)
	}
}
