// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	catalog::flow::FlowNodeId,
	identifier::{ColumnIdentifier, ColumnPrimitive},
	resolved::{ResolvedColumn, ResolvedPrimitive},
};
use reifydb_rql::{
	expression::{ColumnExpression, Expression},
	flow::{conversion::to_owned_physical_plan, node::FlowNodeType::Distinct},
	plan::physical::{DistinctNode, PhysicalPlan},
};
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_type::{Result, fragment::Fragment};

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct DistinctCompiler {
	pub input: Box<PhysicalPlan>,
	pub columns: Vec<ResolvedColumn>,
}

impl From<DistinctNode> for DistinctCompiler {
	fn from(node: DistinctNode) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			columns: node.columns.into_iter().collect(),
		}
	}
}

// Helper function to convert ResolvedColumn to ColumnIdentifier for expression system
fn resolved_to_column_identifier(resolved: ResolvedColumn) -> ColumnIdentifier {
	let primitive = match resolved.primitive() {
		ResolvedPrimitive::Table(t) => ColumnPrimitive::Primitive {
			namespace: Fragment::internal(t.namespace().name()),
			primitive: Fragment::internal(t.name()),
		},
		ResolvedPrimitive::View(v) => ColumnPrimitive::Primitive {
			namespace: Fragment::internal(v.namespace().name()),
			primitive: Fragment::internal(v.name()),
		},
		ResolvedPrimitive::RingBuffer(r) => ColumnPrimitive::Primitive {
			namespace: Fragment::internal(r.namespace().name()),
			primitive: Fragment::internal(r.name()),
		},
		_ => ColumnPrimitive::Alias(Fragment::internal("_unknown")),
	};

	ColumnIdentifier {
		primitive,
		name: Fragment::internal(resolved.name()),
	}
}

impl CompileOperator for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut CommandTransaction) -> Result<FlowNodeId> {
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
