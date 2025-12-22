// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use std::sync::Arc;

use reifydb_core::{
	interface::ResolvedColumn,
	value::column::{Column, Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{Expression, column_name_from_expression};
use reifydb_type::{Fragment, diagnostic::query::extend_duplicate_column, return_error};
use tracing::instrument;

use crate::{
	StandardTransaction,
	evaluate::{
		TargetColumn,
		column::{ColumnEvaluationContext, evaluate},
	},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct ExtendNode {
	input: Box<ExecutionPlan>,
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<ExecutionContext>>,
}

impl ExtendNode {
	pub fn new(input: Box<ExecutionPlan>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

#[async_trait]
impl QueryNode for ExtendNode {
	#[instrument(name = "query::extend::initialize", level = "trace", skip_all)]
	async fn initialize<'a>(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx).await?;
		Ok(())
	}

	#[instrument(name = "query::extend::next", level = "trace", skip_all)]
	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "ExtendNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			columns,
		}) = self.input.next(rx, ctx).await?
		{
			// Start with all existing columns (EXTEND preserves
			// everything)
			let row_count = columns.row_count();
			let mut new_columns = columns.into_iter().collect::<Vec<_>>();

			// Add the new derived columns
			// Clone expressions to avoid lifetime issues
			let expressions = self.expressions.clone();
			for expr in expressions.iter() {
				// Create evaluation context inline to avoid lifetime issues
				let mut eval_ctx = ColumnEvaluationContext {
					target: None,
					columns: Columns::new(new_columns.clone()),
					row_count,
					take: None,
					params: &ctx.params,
					stack: &ctx.stack,
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

			// Transmute the vector to extend its lifetime
			// SAFETY: The columns come from either the input (already transmuted to 'a)
			// via into_iter() or from column() which returns Column, so all columns
			// genuinely have lifetime 'a through the query execution
			let new_columns = unsafe { std::mem::transmute::<Vec<Column>, Vec<Column>>(new_columns) };

			// Create layout combining existing and new columns only
			// once For extend, we preserve all input columns
			// plus the new expressions
			if self.headers.is_none() {
				let mut all_headers = if let Some(input_headers) = self.input.headers() {
					input_headers.columns.clone()
				} else {
					vec![]
				};

				let new_names: Vec<Fragment> =
					expressions.iter().map(column_name_from_expression).collect();

				// Check for duplicate column names against existing columns
				for new_name in &new_names {
					for existing_name in &all_headers {
						if new_name.text() == existing_name.text() {
							return_error!(extend_duplicate_column(new_name.text()));
						}
					}
				}

				// Check for duplicates within the new column names themselves
				for i in 0..new_names.len() {
					for j in (i + 1)..new_names.len() {
						if new_names[i].text() == new_names[j].text() {
							return_error!(extend_duplicate_column(new_names[i].text()));
						}
					}
				}

				all_headers.extend(new_names);

				self.headers = Some(ColumnHeaders {
					columns: all_headers,
				});
			}

			return Ok(Some(Batch {
				columns: Columns::new(new_columns),
			}));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

pub(crate) struct ExtendWithoutInputNode {
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<ExecutionContext>>,
}

impl ExtendWithoutInputNode {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			headers: None,
			context: None,
		}
	}
}

#[async_trait]
impl QueryNode for ExtendWithoutInputNode {
	#[instrument(name = "query::extend::noinput::initialize", level = "trace", skip_all)]
	async fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	#[instrument(name = "query::extend::noinput::next", level = "trace", skip_all)]
	async fn next<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "ExtendWithoutInputNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let columns = Columns::empty();
		let mut new_columns = Vec::with_capacity(self.expressions.len());

		// Clone expressions to avoid lifetime issues
		let expressions = self.expressions.clone();
		for expr in expressions.iter() {
			let evaluation_context = ColumnEvaluationContext {
				target: None,
				columns: columns.clone(),
				row_count: 1,
				take: None,
				params: &stored_ctx.params,
				stack: &stored_ctx.stack,
				is_aggregate_context: false,
			};

			let column = evaluate(&evaluation_context, expr)?;
			new_columns.push(column);
		}

		let column_names: Vec<Fragment> = expressions.iter().map(column_name_from_expression).collect();

		// Check for duplicate column names within the new columns
		for i in 0..column_names.len() {
			for j in (i + 1)..column_names.len() {
				if column_names[i].text() == column_names[j].text() {
					return_error!(extend_duplicate_column(column_names[i].text()));
				}
			}
		}

		self.headers = Some(ColumnHeaders {
			columns: column_names,
		});

		// Transmute the vector to extend its lifetime
		// SAFETY: The columns either come from the input (already transmuted to 'a)
		// or from evaluate() which returns Column, so they all genuinely have
		// lifetime 'a through the query execution
		let new_columns = unsafe { std::mem::transmute::<Vec<Column>, Vec<Column>>(new_columns) };

		Ok(Some(Batch {
			columns: Columns::new(new_columns),
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
