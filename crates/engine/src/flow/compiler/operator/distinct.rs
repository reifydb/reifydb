// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::store::ttl::create::create_operator_ttl;
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		identifier::{ColumnIdentifier, ColumnShape},
		resolved::{ResolvedColumn, ResolvedShape},
	},
	row::Ttl,
};
use reifydb_rql::{
	expression::{ColumnExpression, Expression},
	flow::node::FlowNodeType::Distinct,
	nodes::DistinctNode,
	query::QueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment};

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct DistinctCompiler {
	pub input: Box<QueryPlan>,
	pub columns: Vec<ResolvedColumn>,
	pub ttl: Option<Ttl>,
}

impl From<DistinctNode> for DistinctCompiler {
	fn from(node: DistinctNode) -> Self {
		Self {
			input: node.input,
			columns: node.columns.into_iter().collect(),
			ttl: node.ttl,
		}
	}
}

// Helper function to convert ResolvedColumn to ColumnIdentifier for expression system
fn resolved_to_column_identifier(resolved: ResolvedColumn) -> ColumnIdentifier {
	let shape = match resolved.shape() {
		ResolvedShape::Table(t) => ColumnShape::Qualified {
			namespace: Fragment::internal(t.namespace().name()),
			name: Fragment::internal(t.name()),
		},
		ResolvedShape::View(v) => ColumnShape::Qualified {
			namespace: Fragment::internal(v.namespace().name()),
			name: Fragment::internal(v.name()),
		},
		ResolvedShape::RingBuffer(r) => ColumnShape::Qualified {
			namespace: Fragment::internal(r.namespace().name()),
			name: Fragment::internal(r.name()),
		},
		_ => ColumnShape::Alias(Fragment::internal("_unknown")),
	};

	ColumnIdentifier {
		shape,
		name: Fragment::internal(resolved.name()),
	}
}

impl CompileOperator for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		// Convert resolved columns to column expressions via ColumnIdentifier
		let expressions: Vec<Expression> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(resolved_to_column_identifier(col))))
			.collect();

		let ttl = self.ttl.clone();
		let node_id = compiler.add_node(
			txn,
			Distinct {
				expressions,
				ttl: self.ttl,
			},
		)?;

		if let Some(ttl) = ttl
			&& let Transaction::Admin(admin) = txn
		{
			create_operator_ttl(admin, node_id, &ttl)?;
		}

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
