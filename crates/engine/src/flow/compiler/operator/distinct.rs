// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	catalog::flow::FlowNodeId,
	identifier::{ColumnIdentifier, ColumnSchema},
	resolved::{ResolvedColumn, ResolvedSchema},
};
use reifydb_rql::{
	expression::{ColumnExpression, Expression},
	flow::node::FlowNodeType::Distinct,
	nodes::DistinctNode,
	query::QueryPlan,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{Result, fragment::Fragment};

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct DistinctCompiler {
	pub input: Box<QueryPlan>,
	pub columns: Vec<ResolvedColumn>,
}

impl From<DistinctNode> for DistinctCompiler {
	fn from(node: DistinctNode) -> Self {
		Self {
			input: node.input,
			columns: node.columns.into_iter().collect(),
		}
	}
}

// Helper function to convert ResolvedColumn to ColumnIdentifier for expression system
fn resolved_to_column_identifier(resolved: ResolvedColumn) -> ColumnIdentifier {
	let schema = match resolved.schema() {
		ResolvedSchema::Table(t) => ColumnSchema::Qualified {
			namespace: Fragment::internal(t.namespace().name()),
			name: Fragment::internal(t.name()),
		},
		ResolvedSchema::View(v) => ColumnSchema::Qualified {
			namespace: Fragment::internal(v.namespace().name()),
			name: Fragment::internal(v.name()),
		},
		ResolvedSchema::RingBuffer(r) => ColumnSchema::Qualified {
			namespace: Fragment::internal(r.namespace().name()),
			name: Fragment::internal(r.name()),
		},
		_ => ColumnSchema::Alias(Fragment::internal("_unknown")),
	};

	ColumnIdentifier {
		schema,
		name: Fragment::internal(resolved.name()),
	}
}

impl CompileOperator for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		// Convert resolved columns to column expressions via ColumnIdentifier
		let expressions: Vec<Expression> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(resolved_to_column_identifier(col))))
			.collect();

		let node_id = compiler.add_node(
			txn,
			Distinct {
				expressions,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
