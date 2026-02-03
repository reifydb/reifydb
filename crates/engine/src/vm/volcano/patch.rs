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
	evaluate::{ColumnEvaluationContext, column::evaluate},
	vm::volcano::query::{QueryContext, QueryNode, QueryPlan},
};

/// PatchNode merges assignment values with original row values.
/// Unlike ExtendNode which adds new columns, PatchNode replaces
/// columns that have matching names in the assignments.
pub(crate) struct PatchNode {
	input: Box<QueryPlan>,
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<QueryContext>>,
}

impl PatchNode {
	pub fn new(input: Box<QueryPlan>, expressions: Vec<Expression>) -> Self {
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
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "volcano::patch::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "PatchNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		while let Some(columns) = self.input.next(rx, ctx)? {
			let row_count = columns.row_count();
			let row_numbers = columns.row_numbers.to_vec();

			let patch_names: Vec<Fragment> =
				self.expressions.iter().map(column_name_from_expression).collect();

			let expressions = self.expressions.clone();
			let mut patch_columns = Vec::with_capacity(expressions.len());
			for expr in expressions.iter() {
				let mut eval_ctx = ColumnEvaluationContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: &ctx.params,
					symbol_table: &ctx.stack,
					is_aggregate_context: false,
				};

				if let (Expression::Alias(alias_expr), Some(source)) = (expr, &stored_ctx.source) {
					let alias_name = alias_expr.alias.name();

					if let Some(table_column) =
						source.columns().iter().find(|col| col.name == alias_name)
					{
						let column_ident = Fragment::internal(&table_column.name);
						let resolved_column = ResolvedColumn::new(
							column_ident,
							source.clone(),
							table_column.clone(),
						);
						eval_ctx.target = Some(TargetColumn::Resolved(resolved_column));
					}
				}

				let column = evaluate(&eval_ctx, expr, &stored_ctx.services.functions)?;
				patch_columns.push(column);
			}

			let mut result_columns = Vec::new();
			let mut result_headers = Vec::new();

			for original_col in columns.into_iter() {
				let original_name = original_col.name().text();

				if let Some(patch_idx) = patch_names.iter().position(|n| n.text() == original_name) {
					result_columns.push(patch_columns[patch_idx].clone());
					result_headers.push(patch_names[patch_idx].clone());
				} else {
					result_headers.push(original_col.name().clone());
					result_columns.push(original_col);
				}
			}

			for (patch_idx, patch_name) in patch_names.iter().enumerate() {
				if !result_headers.iter().any(|h| h.text() == patch_name.text()) {
					result_columns.push(patch_columns[patch_idx].clone());
					result_headers.push(patch_name.clone());
				}
			}

			if self.headers.is_none() {
				self.headers = Some(ColumnHeaders {
					columns: result_headers.clone(),
				});
			}

			return Ok(Some(Columns::with_row_numbers(result_columns, row_numbers)));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}
