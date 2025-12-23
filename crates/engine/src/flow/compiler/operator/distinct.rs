// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result,
	interface::{ColumnSource, FlowNodeId, ResolvedColumn, ResolvedSource, identifier::ColumnIdentifier},
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
	let source = match resolved.source() {
		ResolvedSource::Table(t) => ColumnSource::Source {
			namespace: Fragment::internal(t.namespace().name()),
			source: Fragment::internal(t.name()),
		},
		ResolvedSource::View(v) => ColumnSource::Source {
			namespace: Fragment::internal(v.namespace().name()),
			source: Fragment::internal(v.name()),
		},
		ResolvedSource::RingBuffer(r) => ColumnSource::Source {
			namespace: Fragment::internal(r.namespace().name()),
			source: Fragment::internal(r.name()),
		},
		_ => ColumnSource::Alias(Fragment::internal("_unknown")),
	};

	ColumnIdentifier {
		source,
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
