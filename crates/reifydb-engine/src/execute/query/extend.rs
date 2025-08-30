// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	ColumnDescriptor,
	interface::{
		QueryTransaction, Transaction, evaluate::expression::Expression,
	},
};

use crate::{
	StandardCommandTransaction,
	columnar::{Columns, layout::ColumnsLayout},
	evaluate::{EvaluationContext, evaluate},
	execute::{
		Batch, ExecutionContext, ExecutionPlan,
		query::layout::derive_columns_column_layout,
	},
};

pub(crate) struct ExtendNode {
	input: Box<ExecutionPlan>,
	expressions: Vec<Expression>,
	layout: Option<ColumnsLayout>,
}

impl ExtendNode {
	pub fn new(
		input: Box<ExecutionPlan>,
		expressions: Vec<Expression>,
	) -> Self {
		Self {
			input,
			expressions,
			layout: None,
		}
	}

	fn create_evaluation_context<'a>(
		&self,
		expr: &Expression,
		ctx: &'a ExecutionContext,
		columns: Columns,
		row_count: usize,
	) -> EvaluationContext<'a> {
		let mut result = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
			columns,
			row_count,
			take: None,
			params: &ctx.params,
		};

		// Check if this is an alias expression and we have table
		// information
		if let (Expression::Alias(alias_expr), Some(table)) =
			(expr, &ctx.table)
		{
			let alias_name = alias_expr.alias.name();

			// Find the matching column in the table schema
			if let Some(table_column) = table
				.columns
				.iter()
				.find(|col| col.name == alias_name)
			{
				// Extract ColumnPolicyKind from ColumnPolicy
				let policy_kinds: Vec<_> = table_column
					.policies
					.iter()
					.map(|policy| policy.policy.clone())
					.collect();

				let target_column = ColumnDescriptor::new()
					.with_table(&table.name)
					.with_column(&table_column.name)
					.with_column_type(table_column.ty)
					.with_policies(policy_kinds.clone());

				result.target_column = Some(target_column);
				result.column_policies = policy_kinds;
			}
		}

		result
	}
}

impl ExtendNode {
	pub(crate) fn next<T: Transaction>(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut StandardCommandTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		while let Some(Batch {
			columns,
		}) = self.input.next(ctx, rx)?
		{
			// Start with all existing columns (EXTEND preserves
			// everything)
			let row_count = columns.row_count();
			let mut new_columns =
				columns.into_iter().collect::<Vec<_>>();

			// Add the new derived columns
			for expr in &self.expressions {
				let column = evaluate(
					&self.create_evaluation_context(
						expr,
						ctx,
						Columns::new(
							new_columns.clone(),
						),
						row_count,
					),
					expr,
				)?;

				new_columns.push(column);
			}

			// Create layout combining existing and new columns only
			// once For extend, we preserve all input columns
			// plus the new expressions
			if self.layout.is_none() {
				let layout = if let Some(input_layout) =
					self.input.layout()
				{
					// Combine input layout with new
					// expression layout
					let new_expressions_layout = derive_columns_column_layout(
						&self.expressions,
						ctx.preserve_row_numbers,
					);
					input_layout.extend(
						&new_expressions_layout,
					)?
				} else {
					derive_columns_column_layout(
						&self.expressions,
						ctx.preserve_row_numbers,
					)
				};

				self.layout = Some(layout);
			}

			return Ok(Some(Batch {
				columns: Columns::new(new_columns),
			}));
		}
		Ok(None)
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone().or(self.input.layout())
	}
}

pub(crate) struct ExtendWithoutInputNode {
	expressions: Vec<Expression>,
	layout: Option<ColumnsLayout>,
}

impl ExtendWithoutInputNode {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			layout: None,
		}
	}
}

impl ExtendWithoutInputNode {
	pub(crate) fn next<T: Transaction>(
		&mut self,
		ctx: &ExecutionContext,
		_rx: &mut StandardCommandTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		if self.layout.is_some() {
			return Ok(None);
		}

		// Without input, this behaves like MAP without input
		// (generates a single row with the computed expressions)
		let columns = Columns::empty();
		let mut new_columns =
			Vec::with_capacity(self.expressions.len());

		for expr in &self.expressions {
			let evaluation_context = EvaluationContext {
				target_column: None,
				column_policies: Vec::new(),
				columns: columns.clone(),
				row_count: 1, // Generate single row
				take: None,
				params: &ctx.params,
			};

			let column = evaluate(&evaluation_context, expr)?;
			new_columns.push(column);
		}

		let layout = derive_columns_column_layout(
			&self.expressions,
			ctx.preserve_row_numbers,
		);

		self.layout = Some(layout);

		Ok(Some(Batch {
			columns: Columns::new(new_columns),
		}))
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}
