// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Distinct;
use reifydb_core::interface::{
	ColumnSource, CommandTransaction, FlowNodeId, ResolvedColumn, ResolvedSource, identifier::ColumnIdentifier,
};
use reifydb_type::Fragment;

use super::super::{CompileOperator, FlowCompiler, FlowNodeType, conversion::to_owned_physical_plan};
use crate::{
	Result,
	expression::{ColumnExpression, Expression},
	plan::physical::{DistinctNode, PhysicalPlan},
};

pub(crate) struct DistinctCompiler {
	pub input: Box<PhysicalPlan>,
	pub columns: Vec<ResolvedColumn>,
}

impl From<DistinctNode> for DistinctCompiler {
	fn from(node: DistinctNode) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			columns: node.columns.into_iter().map(|c| c).collect(),
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

impl<T: CommandTransaction> CompileOperator<T> for DistinctCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input).await?;

		// Convert resolved columns to column expressions via ColumnIdentifier
		let expressions: Vec<Expression> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(resolved_to_column_identifier(col))))
			.collect();

		compiler.build_node(Distinct {
			expressions,
		})
		.with_input(input_node)
		.build()
		.await
	}
}
