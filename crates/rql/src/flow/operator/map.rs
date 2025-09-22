// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeDef, FlowNodeType::Operator, OperatorType::Map},
	interface::{
		ColumnDef, ColumnId, ColumnIndex, CommandTransaction, FlowNodeId, evaluate::expression::Expression,
	},
};
use reifydb_type::TypeConstraint;

use super::super::{
	CompileOperator, FlowCompiler,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	plan::physical::{MapNode, PhysicalPlan},
};

pub(crate) struct MapCompiler {
	pub input: Option<Box<PhysicalPlan<'static>>>,
	pub expressions: Vec<Expression<'static>>,
}

impl MapCompiler {
	/// Compute the output namespace from Map expressions
	pub(crate) fn compute_output_schema(&self, input_schema: &FlowNodeDef) -> FlowNodeDef {
		let mut columns = Vec::new();

		for (idx, expr) in self.expressions.iter().enumerate() {
			// Extract column name from expression
			let column_name = match expr {
				Expression::Alias(alias) => {
					// Use the alias name
					alias.alias.0.text().to_string()
				}
				Expression::Column(col) => {
					// Use the column name
					col.0.name.text().to_string()
				}
				Expression::AccessSource(access) => {
					// Use the column part
					access.column.name.text().to_string()
				}
				_ => {
					// For other expressions, generate a
					// name
					format!("column_{}", idx)
				}
			};

			// For now, we'll use a generic type since we don't have
			// type inference In a real implementation, we'd
			// infer the type from the expression
			columns.push(ColumnDef {
				id: ColumnId(idx as u64),
				name: column_name,
				constraint: TypeConstraint::unconstrained(reifydb_type::Type::Utf8),
				policies: vec![],
				index: ColumnIndex(idx as u16),
				auto_increment: false,
			});
		}

		// Preserve namespace and source names from input if available
		FlowNodeDef {
			columns,
			namespace_name: input_schema.namespace_name.clone(),
			source_name: None, /* Map output doesn't have a
			                    * direct source */
		}
	}
}

impl<'a> From<MapNode<'a>> for MapCompiler {
	fn from(node: MapNode<'a>) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			expressions: to_owned_expressions(node.map),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for MapCompiler {
	fn compile(mut self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		// Compile input and get its namespace
		let (input_node, input_schema) = if let Some(input) = self.input.take() {
			let (node_id, namespace) = compiler.compile_plan_with_schema(*input)?;
			(Some(node_id), namespace)
		} else {
			(None, FlowNodeDef::empty())
		};

		// Compute output namespace based on Map expressions
		let output_schema = self.compute_output_schema(&input_schema);

		let mut builder = compiler.build_node(Operator {
			operator: Map {
				expressions: self.expressions,
			},
			input_schemas: vec![input_schema],
			output_schema,
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build()
	}
}
