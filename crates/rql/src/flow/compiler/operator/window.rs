// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, operator_with::WindowWith};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

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
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub with: WindowWith,
}

impl From<WindowNode> for WindowCompiler {
	fn from(node: WindowNode) -> Self {
		Self {
			input: node.input,
			group_by: node.group_by,
			aggregations: node.aggregations,
			with: node.with,
		}
	}
}

impl CompileOperator for WindowCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		validate_flow_aggregations(
			&compiler.routines,
			&self.aggregations,
			AggregateContext::Windowed,
			Some(&self.with.kind),
		)?;

		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Window {
				group_by: self.group_by,
				aggregations: self.aggregations,
				with: self.with,
			},
		)?;

		if let Some(input_node) = input_node {
			compiler.add_edge(txn, &input_node, &node_id)?;
		}

		Ok(node_id)
	}
}
