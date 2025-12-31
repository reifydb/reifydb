// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Result,
	interface::{ColumnPrimitive, FlowNodeId, ResolvedColumn, ResolvedPrimitive, identifier::ColumnIdentifier},
};
use reifydb_rql::{
	expression::{ColumnExpression, Expression},
	flow::{FlowNodeType::Distinct, conversion::to_owned_physical_plan},
	plan::physical::{DistinctNode, PhysicalPlan},
};
use reifydb_type::Fragment;

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input).await?;

		// Convert resolved columns to column expressions via ColumnIdentifier
		let expressions: Vec<Expression> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(resolved_to_column_identifier(col))))
			.collect();

		let node_id = compiler
			.add_node(
				txn,
				Distinct {
					expressions,
				},
			)
			.await?;

		compiler.add_edge(txn, &input_node, &node_id).await?;
		Ok(node_id)
	}
}
