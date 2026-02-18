// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{expression::Expression, flow::node::FlowNodeType::Filter, nodes::FilterNode, query::QueryPlan};
use reifydb_transaction::transaction::{AsTransaction, admin::AdminTransaction};
use reifydb_type::Result;

use crate::{
	flow::compiler::{CompileOperator, FlowCompiler},
	vm::volcano::{compile::extract_resolved_source, filter::resolve_is_variant_tags},
};

pub(crate) struct FilterCompiler {
	pub input: Box<QueryPlan>,
	pub conditions: Vec<Expression>,
}

impl From<FilterNode> for FilterCompiler {
	fn from(node: FilterNode) -> Self {
		Self {
			input: node.input,
			conditions: node.conditions,
		}
	}
}

impl CompileOperator for FilterCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let mut conditions = self.conditions;
		if let Some(source) = extract_resolved_source(&self.input) {
			let mut tx = txn.as_transaction();
			for expr in &mut conditions {
				resolve_is_variant_tags(expr, &source, &compiler.catalog, &mut tx)?;
			}
		}

		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Filter {
				conditions,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
