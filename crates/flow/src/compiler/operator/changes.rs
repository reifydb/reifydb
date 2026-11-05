// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::operation::changes_on_column_not_found, flow::operator::OperatorDef::Changes,
	interface::catalog::flow::OperatorId, operator_with::ChangesWith,
};
use reifydb_rql::{nodes::ChangesNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error, fragment::Fragment};

use crate::compiler::{CompileOperator, FlowCompiler, operator::join::output_column_names};

pub(crate) struct ChangesCompiler {
	pub input: Box<QueryPlan>,
	pub with: ChangesWith,
}

impl From<ChangesNode> for ChangesCompiler {
	fn from(node: ChangesNode) -> Self {
		Self {
			input: node.input,
			with: node.with,
		}
	}
}

impl CompileOperator for ChangesCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		if let Some(columns) = output_column_names(&self.input) {
			for name in &self.with.update_on {
				if !columns.contains(name) {
					return Err(error!(changes_on_column_not_found(Fragment::internal(name))));
				}
			}
		}
		let input_node = compiler.compile_plan(txn, *self.input)?;
		let node_id = compiler.add_node(
			txn,
			Changes {
				with: self.with,
			},
		)?;
		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
