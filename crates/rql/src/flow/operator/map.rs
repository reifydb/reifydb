// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeDef, FlowNodeType::Operator, OperatorType::Map},
	interface::{
		ColumnDef, ColumnId, ColumnIndex, CommandTransaction, FlowNodeId, ViewDef,
		evaluate::expression::Expression,
	},
};
use reifydb_type::{Type, TypeConstraint};

use super::super::{
	CompileOperator, FlowCompiler,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::infer_type,
	plan::physical::{MapNode, PhysicalPlan},
};

pub(crate) struct MapCompiler {
	pub input: Option<Box<PhysicalPlan<'static>>>,
	pub expressions: Vec<Expression<'static>>,
}

/// Infer the type of an expression based on input schema
fn infer_expression_type(expr: &Expression, input_schema: &FlowNodeDef) -> Type {
	use reifydb_type::Type;

	match expr {
		Expression::Alias(alias_expr) => {
			// For aliases, infer from the inner expression
			infer_expression_type(&alias_expr.expression, input_schema)
		}
		Expression::Column(col_expr) => {
			// Look up column type in input schema
			let col_name = col_expr.0.name.text();
			for col in &input_schema.columns {
				if col.name == col_name {
					return col.constraint.get_type();
				}
			}
			// Default to Utf8 if not found
			Type::Utf8
		}
		Expression::AccessSource(access_expr) => {
			// Look up column type in input schema
			let col_name = access_expr.column.name.text();
			for col in &input_schema.columns {
				if col.name == col_name {
					return col.constraint.get_type();
				}
			}
			// Default to Utf8 if not found
			Type::Undefined
		}
		Expression::Constant(_const_expr) => {
			// Constants typically have their type defined
			// For now, default to Utf8
			Type::Utf8
		}
		Expression::Add(_)
		| Expression::Sub(_)
		| Expression::Mul(_)
		| Expression::Div(_)
		| Expression::Rem(_) => {
			// Arithmetic operations - use Int4 to match view expectations
			Type::Int4
		}
		Expression::And(_) | Expression::Or(_) | Expression::Xor(_) => {
			// Boolean operations
			Type::Boolean
		}
		Expression::Equal(_)
		| Expression::NotEqual(_)
		| Expression::GreaterThan(_)
		| Expression::GreaterThanEqual(_)
		| Expression::LessThan(_)
		| Expression::LessThanEqual(_) => {
			// Comparison operations
			Type::Boolean
		}
		_ => {
			// Default to Utf8 for unknown expressions
			Type::Utf8
		}
	}
}

impl MapCompiler {
	/// Compute the output namespace from Map expressions
	pub(crate) fn compute_output_schema(&self, input_schema: &FlowNodeDef, sink: Option<&ViewDef>) -> FlowNodeDef {
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

			// Determine the column type
			let column_type = if let Some(view) = sink {
				// Terminal node: use view schema types
				if let Some(view_column) = view.columns.get(idx) {
					view_column.constraint.get_type()
				} else {
					infer_expression_type(expr, input_schema)
				}
			} else {
				// Intermediate node: infer type from expression
				infer_expression_type(expr, input_schema)
			};

			columns.push(ColumnDef {
				id: ColumnId(idx as u64),
				name: column_name,
				constraint: TypeConstraint::unconstrained(column_type),
				policies: vec![],
				index: ColumnIndex(idx as u16),
				auto_increment: false,
			});
		}

		// Preserve namespace and source names from input if available
		FlowNodeDef {
			columns,
			namespace_name: input_schema.namespace_name.clone(),
			source_name: None,
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
		// Compile input and get its definition
		let (input_node, input_schema) = if let Some(input) = self.input.take() {
			let (node_id, definition) = compiler.compile_plan_with_definition(*input)?;
			(Some(node_id), definition)
		} else {
			(None, FlowNodeDef::empty())
		};

		// Compute output namespace based on Map expressions
		// Pass the sink view schema for terminal nodes
		let output_schema = self.compute_output_schema(&input_schema, compiler.sink.as_ref());

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
