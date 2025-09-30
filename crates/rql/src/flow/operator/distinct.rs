// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeDef, FlowNodeType::Operator, OperatorType::Distinct},
	interface::{
		CommandTransaction, FlowNodeId,
		evaluate::expression::{ColumnExpression, Expression},
		identifier::{ColumnIdentifier, ColumnSource},
		resolved::{ResolvedColumn, ResolvedSource},
	},
};
use reifydb_type::Fragment;

use super::super::{CompileOperator, FlowCompiler, conversion::to_owned_physical_plan};
use crate::{
	Result,
	plan::physical::{DistinctNode, PhysicalPlan},
};

pub(crate) struct DistinctCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub columns: Vec<ResolvedColumn<'static>>,
}

impl<'a> From<DistinctNode<'a>> for DistinctCompiler {
	fn from(node: DistinctNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			columns: node.columns.into_iter().map(|c| c.to_static()).collect(),
		}
	}
}

// Helper function to convert ResolvedColumn to ColumnIdentifier for expression system
fn resolved_to_column_identifier(resolved: ResolvedColumn<'static>) -> ColumnIdentifier<'static> {
	let source = match resolved.source() {
		ResolvedSource::Table(t) => ColumnSource::Source {
			namespace: Fragment::owned_internal(t.namespace().name()),
			source: Fragment::owned_internal(t.name()),
		},
		ResolvedSource::View(v) => ColumnSource::Source {
			namespace: Fragment::owned_internal(v.namespace().name()),
			source: Fragment::owned_internal(v.name()),
		},
		ResolvedSource::RingBuffer(r) => ColumnSource::Source {
			namespace: Fragment::owned_internal(r.namespace().name()),
			source: Fragment::owned_internal(r.name()),
		},
		_ => ColumnSource::Alias(Fragment::owned_internal("_unknown")),
	};

	ColumnIdentifier {
		source,
		name: Fragment::owned_internal(resolved.name()),
	}
}

impl<T: CommandTransaction> CompileOperator<T> for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		// Convert resolved columns to column expressions via ColumnIdentifier
		let expressions: Vec<Expression<'static>> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(resolved_to_column_identifier(col))))
			.collect();

		compiler.build_node(Operator {
			operator: Distinct {
				expressions,
			},
			input_schemas: vec![FlowNodeDef::empty()],
			output_schema: FlowNodeDef::empty(),
		})
		.with_input(input_node)
		.build()
	}
}
