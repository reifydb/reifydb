// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{expression::Expression, flow::operator::OperatorDef::Filter, interface::catalog::flow::OperatorId};
use reifydb_rql::{
	expression::variant::resolve_is_variant_tags,
	nodes::FilterNode,
	query::{QueryPlan, extract_resolved_source},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::compiler::{CompileOperator, FlowCompiler};

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let mut conditions = self.conditions;
		if let Some(source) = extract_resolved_source(&self.input) {
			let mut tx = txn.reborrow();
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
