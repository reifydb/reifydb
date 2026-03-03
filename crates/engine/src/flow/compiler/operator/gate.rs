// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{expression::Expression, flow::node::FlowNodeType::Gate, nodes::GateNode, query::QueryPlan};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct GateCompiler {
	pub input: Box<QueryPlan>,
	pub conditions: Vec<Expression>,
}

impl From<GateNode> for GateCompiler {
	fn from(node: GateNode) -> Self {
		Self {
			input: node.input,
			conditions: node.conditions,
		}
	}
}

impl CompileOperator for GateCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let conditions = self.conditions;

		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Gate {
				conditions,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
