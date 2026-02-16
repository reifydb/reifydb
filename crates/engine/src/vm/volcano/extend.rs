// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::query::extend_duplicate_column,
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};
use tracing::instrument;

use crate::{
	expression::{
		cast::cast_column_data,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	transform::{Transform, context::TransformContext},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct ExtendNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl ExtendNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for ExtendNode {
	#[instrument(name = "volcano::extend::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbol_table: &ctx.stack,
		};
		let compiled = self
			.expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
			.collect();
		self.context = Some((Arc::new(ctx.clone()), compiled));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "volcano::extend::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "ExtendNode::next() called before initialize()");

		while let Some(columns) = self.input.next(rx, ctx)? {
			let stored_ctx = &self.context.as_ref().unwrap().0;
			let transform_ctx = TransformContext {
				functions: &stored_ctx.services.functions,
				clock: &stored_ctx.services.clock,
				params: &stored_ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;

			if self.headers.is_none() {
				let mut all_headers = if let Some(input_headers) = self.input.headers() {
					input_headers.columns.clone()
				} else {
					let input_column_count = result.len() - self.expressions.len();
					result.iter().take(input_column_count).map(|c| c.name().clone()).collect()
				};

				let new_names: Vec<Fragment> =
					self.expressions.iter().map(column_name_from_expression).collect();
				all_headers.extend(new_names);

				self.headers = Some(ColumnHeaders {
					columns: all_headers,
				});
			}

			return Ok(Some(result));
		}
		if self.headers.is_none() {
			if let Some(input_headers) = self.input.headers() {
				let mut all_headers = input_headers.columns.clone();
				let new_names: Vec<Fragment> =
					self.expressions.iter().map(column_name_from_expression).collect();

				for new_name in &new_names {
					for existing_name in &all_headers {
						if new_name.text() == existing_name.text() {
							return_error!(extend_duplicate_column(new_name.text()));
						}
					}
				}
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
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

impl Transform for ExtendNode {
	fn apply(&self, ctx: &TransformContext, input: Columns) -> reifydb_type::Result<Columns> {
		let (stored_ctx, compiled) =
			self.context.as_ref().expect("ExtendNode::apply() called before initialize()");

		let row_count = input.row_count();
		let row_numbers = input.row_numbers.to_vec();

		// Collect existing column names for duplicate checking
		let existing_names: Vec<Fragment> = input.iter().map(|c| c.name().clone()).collect();

		let mut new_columns = input.into_iter().collect::<Vec<_>>();

		let mut new_names = Vec::with_capacity(compiled.len());
		for (expr, compiled_expr) in self.expressions.iter().zip(compiled.iter()) {
			let mut exec_ctx = EvalContext {
				target: None,
				columns: Columns::new(new_columns.clone()),
				row_count,
				take: None,
				params: ctx.params,
				symbol_table: &stored_ctx.stack,
				is_aggregate_context: false,
				functions: ctx.functions,
				clock: ctx.clock,
				arena: None,
			};

			if let (Expression::Alias(alias_expr), Some(source)) = (expr, &stored_ctx.source) {
				let alias_name = alias_expr.alias.name();
				if let Some(table_column) = source.columns().iter().find(|col| col.name == alias_name) {
					let column_ident = Fragment::internal(&table_column.name);
					let resolved_column =
						ResolvedColumn::new(column_ident, source.clone(), table_column.clone());
					exec_ctx.target = Some(TargetColumn::Resolved(resolved_column));
				}
			}

			let mut column = compiled_expr.execute(&exec_ctx)?;

			if let Some(target_type) = exec_ctx.target.as_ref().map(|t| t.column_type()) {
				if column.data.get_type() != target_type {
					let data = cast_column_data(
						&exec_ctx,
						&column.data,
						target_type,
						&expr.lazy_fragment(),
					)?;
					column = reifydb_core::value::column::Column {
						name: column.name,
						data,
					};
				}
			}

			new_columns.push(column);
			new_names.push(column_name_from_expression(expr));
		}

		// Validate no duplicate column names against existing columns
		for new_name in &new_names {
			for existing_name in &existing_names {
				if new_name.text() == existing_name.text() {
					return_error!(extend_duplicate_column(new_name.text()));
				}
			}
		}

		// Validate no duplicates within new columns
		for i in 0..new_names.len() {
			for j in (i + 1)..new_names.len() {
				if new_names[i].text() == new_names[j].text() {
					return_error!(extend_duplicate_column(new_names[i].text()));
				}
			}
		}

		if row_numbers.is_empty() {
			Ok(Columns::new(new_columns))
		} else {
			Ok(Columns::with_row_numbers(new_columns, row_numbers))
		}
	}
}

pub(crate) struct ExtendWithoutInputNode {
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
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

impl QueryNode for ExtendWithoutInputNode {
	#[instrument(name = "volcano::extend::noinput::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbol_table: &ctx.stack,
		};
		let compiled = self
			.expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
			.collect();
		self.context = Some((Arc::new(ctx.clone()), compiled));
		Ok(())
	}

	#[instrument(name = "volcano::extend::noinput::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "ExtendWithoutInputNode::next() called before initialize()");
		let (stored_ctx, compiled) = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let columns = Columns::empty();
		let mut new_columns = Vec::with_capacity(self.expressions.len());

		for compiled_expr in compiled {
			let exec_ctx = EvalContext {
				target: None,
				columns: columns.clone(),
				row_count: 1,
				take: None,
				params: &stored_ctx.params,
				symbol_table: &stored_ctx.stack,
				is_aggregate_context: false,
				functions: &stored_ctx.services.functions,
				clock: &stored_ctx.services.clock,
				arena: None,
			};

			let column = compiled_expr.execute(&exec_ctx)?;
			new_columns.push(column);
		}

		let column_names: Vec<Fragment> = self.expressions.iter().map(column_name_from_expression).collect();

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

		Ok(Some(Columns::new(new_columns)))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
