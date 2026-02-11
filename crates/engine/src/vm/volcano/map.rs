// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	expression::{
		cast::cast_column_data,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct MapNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl MapNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for MapNode {
	#[instrument(name = "volcano::map::initialize", level = "trace", skip_all)]
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

	#[instrument(name = "volcano::map::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "MapNode::next() called before initialize()");
		let (stored_ctx, compiled) = self.context.as_ref().unwrap();

		while let Some(columns) = self.input.next(rx, ctx)? {
			let mut new_columns = Vec::with_capacity(self.expressions.len());

			let row_count = columns.row_count();

			let expressions = &self.expressions;
			for (expr, compiled_expr) in expressions.iter().zip(compiled.iter()) {
				let mut exec_ctx = EvalContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: &stored_ctx.params,
					symbol_table: &stored_ctx.stack,
					is_aggregate_context: false,
					functions: &stored_ctx.services.functions,
					clock: &stored_ctx.services.clock,
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

			return Ok(Some(result_columns));
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
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
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
	#[instrument(name = "volcano::map::noinput::initialize", level = "trace", skip_all)]
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

	#[instrument(name = "volcano::map::noinput::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "MapWithoutInputNode::next() called before initialize()");
		let (stored_ctx, compiled) = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let mut columns = vec![];

		for compiled_expr in compiled {
			let exec_ctx = EvalContext {
				target: None,
				columns: Columns::empty(),
				row_count: 1,
				take: None,
				params: &stored_ctx.params,
				symbol_table: &stored_ctx.stack,
				is_aggregate_context: false,
				functions: &stored_ctx.services.functions,
				clock: &stored_ctx.services.clock,
			};

			let column = compiled_expr.execute(&exec_ctx)?;

			columns.push(column);
		}

		let columns = Columns::new(columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
