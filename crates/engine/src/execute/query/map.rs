// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interface::ResolvedColumn,
	value::column::{Column, Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{Expression, column_name_from_expression};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	evaluate::{
		TargetColumn,
		column::{ColumnEvaluationContext, evaluate},
	},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct MapNode<'a> {
	input: Box<ExecutionPlan<'a>>,
	expressions: Vec<Expression<'a>>,
	headers: Option<ColumnHeaders<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
}

impl<'a> MapNode<'a> {
	pub fn new(input: Box<ExecutionPlan<'a>>, expressions: Vec<Expression<'a>>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl<'a> QueryNode<'a> for MapNode<'a> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "MapNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			columns,
		}) = self.input.next(rx, ctx)?
		{
			let mut new_columns = Vec::with_capacity(self.expressions.len());

			let row_count = columns.row_count();

			// Clone expressions to avoid lifetime issues
			let expressions = self.expressions.clone();
			for expr in &expressions {
				// Create evaluation context inline to avoid lifetime issues
				let mut eval_ctx = ColumnEvaluationContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: &stored_ctx.params,
					stack: &stored_ctx.stack,
				};

				// Check if this is an alias expression and we have source information
				if let (Expression::Alias(alias_expr), Some(source)) = (expr, &stored_ctx.source) {
					let alias_name = alias_expr.alias.name();

					// Find the matching column in the source
					if let Some(table_column) =
						source.columns().iter().find(|col| col.name == alias_name)
					{
						// Create a resolved column with source information
						let column_ident = Fragment::borrowed_internal(&table_column.name);
						let resolved_column = ResolvedColumn::new(
							column_ident,
							source.clone(),
							table_column.clone(),
						);

						eval_ctx.target = Some(TargetColumn::Resolved(resolved_column));
					}
				}

				let column = evaluate(&eval_ctx, expr)?;

				new_columns.push(column);
			}

			// Transmute the vector to extend its lifetime
			// SAFETY: The columns either come from the input (already transmuted to 'a)
			// or from column() which returns Column<'a>, so they all genuinely have
			// lifetime 'a through the query execution
			let new_columns =
				unsafe { std::mem::transmute::<Vec<Column<'_>>, Vec<Column<'a>>>(new_columns) };

			let column_names = expressions.iter().map(column_name_from_expression).collect();
			self.headers = Some(ColumnHeaders {
				columns: column_names,
			});

			// Create new Columns with the original encoded numbers preserved
			let result_columns = if !columns.row_numbers.is_empty() {
				Columns::with_row_numbers(new_columns, columns.row_numbers.to_vec())
			} else {
				Columns::new(new_columns)
			};

			return Ok(Some(Batch {
				columns: result_columns,
			}));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		self.headers.clone().or(self.input.headers())
	}
}

pub(crate) struct MapWithoutInputNode<'a> {
	expressions: Vec<Expression<'a>>,
	headers: Option<ColumnHeaders<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
}

impl<'a> MapWithoutInputNode<'a> {
	pub fn new(expressions: Vec<Expression<'a>>) -> Self {
		Self {
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl<'a> QueryNode<'a> for MapWithoutInputNode<'a> {
	fn initialize(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "MapWithoutInputNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let mut columns = vec![];

		// Clone expressions to avoid lifetime issues
		let expressions = self.expressions.clone();
		for expr in expressions.iter() {
			let column = evaluate(
				&ColumnEvaluationContext {
					target: None,
					columns: Columns::empty(),
					row_count: 1,
					take: None,
					params: &stored_ctx.params,
					stack: &stored_ctx.stack,
				},
				&expr,
			)?;

			columns.push(column);
		}

		// Transmute the columns to extend their lifetime
		// SAFETY: The columns come from evaluate() which returns Column<'a>
		// so they genuinely have lifetime 'a through the query execution
		let columns = unsafe { std::mem::transmute::<Vec<Column<'_>>, Vec<Column<'a>>>(columns) };

		let columns = Columns::new(columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		self.headers.clone()
	}
}
