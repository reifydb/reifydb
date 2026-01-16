// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_transaction::standard::StandardTransaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	evaluate::{ColumnEvaluationContext, column::evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct MapNode {
	input: Box<ExecutionPlan>,
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<ExecutionContext>>,
}

impl MapNode {
	pub fn new(input: Box<ExecutionPlan>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for MapNode {
	#[instrument(name = "query::map::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "query::map::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
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
					is_aggregate_context: false,
				};

				// Check if this is an alias expression and we have source information
				if let (Expression::Alias(alias_expr), Some(source)) = (expr, &stored_ctx.source) {
					let alias_name = alias_expr.alias.name();

					// Find the matching column in the source
					if let Some(table_column) =
						source.columns().iter().find(|col| col.name == alias_name)
					{
						// Create a resolved column with source information
						let column_ident = Fragment::internal(&table_column.name);
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

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

pub(crate) struct MapWithoutInputNode {
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<ExecutionContext>>,
}

impl MapWithoutInputNode {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for MapWithoutInputNode {
	#[instrument(name = "query::map::noinput::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	#[instrument(name = "query::map::noinput::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
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
					is_aggregate_context: false,
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

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
