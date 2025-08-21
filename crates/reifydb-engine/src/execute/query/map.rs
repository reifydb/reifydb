// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	ColumnDescriptor,
	interface::{
		VersionedQueryTransaction, evaluate::expression::Expression,
	},
	value::row_number::ROW_NUMBER_COLUMN_NAME,
};

use crate::{
	columnar::{Columns, layout::ColumnsLayout},
	evaluate::{EvaluationContext, evaluate},
	execute::{
		Batch, ExecutionContext, ExecutionPlan,
		query::layout::derive_columns_column_layout,
	},
};

pub(crate) struct MapNode {
	input: Box<dyn ExecutionPlan>,
	expressions: Vec<Expression>,
	layout: Option<ColumnsLayout>,
}

impl MapNode {
	pub fn new(
		input: Box<dyn ExecutionPlan>,
		expressions: Vec<Expression>,
	) -> Self {
		Self {
			input,
			expressions,
			layout: None,
		}
	}

	/// Creates an EvaluationContext for a specific expression, injecting
	/// target column information when the expression is an alias
	/// expression that targets a table column during UPDATE/INSERT
	/// operations.
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

impl ExecutionPlan for MapNode {
	fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut dyn VersionedQueryTransaction,
	) -> crate::Result<Option<Batch>> {
		while let Some(Batch {
			columns,
		}) = self.input.next(ctx, rx)?
		{
			let mut new_columns =
				Vec::with_capacity(self.expressions.len());

			// Only preserve RowNumber column if the execution
			// context requires it
			if ctx.preserve_row_numbers {
				if let Some(row_number_column) =
					columns.iter().find(|col| {
						col.name() == ROW_NUMBER_COLUMN_NAME
					}) {
					new_columns
						.push(row_number_column
							.clone());
				}
			}

			let row_count = columns.row_count();

			for expr in &self.expressions {
				let column = evaluate(
					&self.create_evaluation_context(
						expr,
						ctx,
						columns.clone(),
						row_count,
					),
					expr,
				)?;

				new_columns.push(column);
			}

			let layout = derive_columns_column_layout(
				&self.expressions,
				ctx.preserve_row_numbers,
			);

			self.layout = Some(layout);

			return Ok(Some(Batch {
				columns: Columns::new(new_columns),
			}));
		}
		Ok(None)
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone().or(self.input.layout())
	}
}

pub(crate) struct MapWithoutInputNode {
	expressions: Vec<Expression>,
	layout: Option<ColumnsLayout>,
}

impl MapWithoutInputNode {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			layout: None,
		}
	}
}

impl ExecutionPlan for MapWithoutInputNode {
	fn next(
		&mut self,
		ctx: &ExecutionContext,
		_rx: &mut dyn VersionedQueryTransaction,
	) -> crate::Result<Option<Batch>> {
		if self.layout.is_some() {
			return Ok(None);
		}

		let mut columns = vec![];

		for expr in self.expressions.iter() {
			let column = evaluate(
				&EvaluationContext {
					target_column: None,
					column_policies: Vec::new(),
					columns: Columns::empty(),
					row_count: 1,
					take: None,
					params: &ctx.params,
				},
				&expr,
			)?;

			columns.push(column);
		}

		let columns = Columns::new(columns);
		self.layout = Some(ColumnsLayout::from_columns(&columns));
		Ok(Some(Batch {
			columns,
		}))
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}
