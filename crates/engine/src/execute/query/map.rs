// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_core::{
	interface::{ResolvedColumn, TargetColumn, Transaction, evaluate::expression::Expression},
	value::column::{Column, Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::column_name_from_expression;
use reifydb_type::{Fragment, Params, ROW_NUMBER_COLUMN_NAME};

use crate::{
	StandardTransaction,
	evaluate::{EvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct MapNode<'a, T: Transaction> {
	input: Box<ExecutionPlan<'a, T>>,
	expressions: Vec<Expression<'a>>,
	headers: Option<ColumnHeaders<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
}

impl<'a, T: Transaction> MapNode<'a, T> {
	pub fn new(input: Box<ExecutionPlan<'a, T>>, expressions: Vec<Expression<'a>>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for MapNode<'a, T> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a, T>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(&mut self, rx: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "MapNode::next() called before initialize()");
		let ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			columns,
		}) = self.input.next(rx)?
		{
			let mut new_columns = Vec::with_capacity(self.expressions.len());

			// Only preserve RowNumber column if the execution
			// context requires it
			if ctx.preserve_row_numbers {
				if let Some(row_number_column) =
					columns.iter().find(|col| col.name() == ROW_NUMBER_COLUMN_NAME)
				{
					new_columns.push(row_number_column.clone());
				}
			}

			let row_count = columns.row_count();

			// Clone expressions to avoid lifetime issues
			let expressions = self.expressions.clone();
			for expr in &expressions {
				// Create evaluation context inline to avoid lifetime issues
				let mut eval_ctx = EvaluationContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: unsafe { std::mem::transmute::<&Params, &'a Params>(&ctx.params) },
				};

				// Check if this is an alias expression and we have source information
				if let (Expression::Alias(alias_expr), Some(source)) = (expr, &ctx.source) {
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
			// or from evaluate() which returns Column<'a>, so they all genuinely have
			// lifetime 'a through the query execution
			let new_columns =
				unsafe { std::mem::transmute::<Vec<Column<'_>>, Vec<Column<'a>>>(new_columns) };

			let column_names = expressions.iter().map(column_name_from_expression).collect();
			self.headers = Some(ColumnHeaders {
				columns: column_names,
			});

			return Ok(Some(Batch {
				columns: Columns::new(new_columns),
			}));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		self.headers.clone().or(self.input.headers())
	}
}

pub(crate) struct MapWithoutInputNode<'a, T: Transaction> {
	expressions: Vec<Expression<'a>>,
	headers: Option<ColumnHeaders<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
	_phantom: PhantomData<T>,
}

impl<'a, T: Transaction> MapWithoutInputNode<'a, T> {
	pub fn new(expressions: Vec<Expression<'a>>) -> Self {
		Self {
			expressions,
			headers: None,
			context: None,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for MapWithoutInputNode<'a, T> {
	fn initialize(
		&mut self,
		_rx: &mut StandardTransaction<'a, T>,
		ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next(&mut self, _rx: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "MapWithoutInputNode::next() called before initialize()");
		let ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let mut columns = vec![];

		// Clone expressions to avoid lifetime issues
		let expressions = self.expressions.clone();
		for expr in expressions.iter() {
			let column = evaluate(
				&EvaluationContext {
					target: None,
					columns: Columns::empty(),
					row_count: 1,
					take: None,
					params: unsafe { std::mem::transmute::<&Params, &'a Params>(&ctx.params) },
				},
				&expr,
			)?;

			columns.push(column);
		}

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
