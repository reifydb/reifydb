// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{Column, columns::Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	Result,
	expression::{
		cast::cast_column_data,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	transform::{Transform, context::TransformContext},
	vm::volcano::query::{QueryContext, QueryNode},
};

/// PatchNode merges assignment values with original row values.
/// Unlike ExtendNode which adds new columns, PatchNode replaces
/// columns that have matching names in the assignments.
pub(crate) struct PatchNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl PatchNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for PatchNode {
	#[instrument(name = "volcano::patch::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
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

	#[instrument(name = "volcano::patch::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "PatchNode::next() called before initialize()");

		while let Some(columns) = self.input.next(rx, ctx)? {
			let stored_ctx = &self.context.as_ref().unwrap().0;
			let transform_ctx = TransformContext {
				functions: &stored_ctx.services.functions,
				clock: &stored_ctx.services.clock,
				params: &stored_ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;

			if self.headers.is_none() {
				let result_headers: Vec<Fragment> = result.iter().map(|c| c.name().clone()).collect();
				self.headers = Some(ColumnHeaders {
					columns: result_headers,
				});
			}

			return Ok(Some(result));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		if let Some(ref headers) = self.headers {
			return Some(headers.clone());
		}

		let input_headers = self.input.headers()?;
		let patch_names: Vec<Fragment> = self.expressions.iter().map(column_name_from_expression).collect();

		let mut result = Vec::new();
		for col in &input_headers.columns {
			if let Some(patch_idx) = patch_names.iter().position(|n| n.text() == col.text()) {
				result.push(patch_names[patch_idx].clone());
			} else {
				result.push(col.clone());
			}
		}

		for patch_name in &patch_names {
			if !result.iter().any(|h| h.text() == patch_name.text()) {
				result.push(patch_name.clone());
			}
		}

		Some(ColumnHeaders {
			columns: result,
		})
	}
}

impl Transform for PatchNode {
	fn apply(&self, ctx: &TransformContext, input: Columns) -> Result<Columns> {
		let (stored_ctx, compiled) =
			self.context.as_ref().expect("PatchNode::apply() called before initialize()");

		let row_count = input.row_count();
		let row_numbers = input.row_numbers.to_vec();

		let patch_names: Vec<Fragment> = self.expressions.iter().map(column_name_from_expression).collect();

		let mut patch_columns = Vec::with_capacity(self.expressions.len());
		for (expr, compiled_expr) in self.expressions.iter().zip(compiled.iter()) {
			let mut exec_ctx = EvalContext {
				target: None,
				columns: input.clone(),
				row_count,
				take: None,
				params: ctx.params,
				symbol_table: &stored_ctx.stack,
				is_aggregate_context: false,
				functions: ctx.functions,
				clock: ctx.clock,
				arena: None,
				identity: stored_ctx.identity,
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
					column = Column {
						name: column.name,
						data,
					};
				}
			}

			patch_columns.push(column);
		}

		let mut result_columns = Vec::new();

		for original_col in input.into_iter() {
			let original_name = original_col.name().text();

			if let Some(patch_idx) = patch_names.iter().position(|n| n.text() == original_name) {
				result_columns.push(patch_columns[patch_idx].clone());
			} else {
				result_columns.push(original_col);
			}
		}

		for (patch_idx, patch_name) in patch_names.iter().enumerate() {
			if !result_columns.iter().any(|c| c.name().text() == patch_name.text()) {
				result_columns.push(patch_columns[patch_idx].clone());
			}
		}

		if row_numbers.is_empty() {
			Ok(Columns::new(result_columns))
		} else {
			Ok(Columns::with_row_numbers(result_columns, row_numbers))
		}
	}
}
