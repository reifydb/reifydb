// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, sync::Arc};

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{ColumnWithName, columns::Columns, headers::ColumnHeaders},
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec};
use tracing::instrument;

use super::NoopNode;
use crate::{
	Result,
	expression::{
		cast::cast_column_data,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::volcano::{
		query::{QueryContext, QueryNode},
		udf::{UdfEvalNode, strip_udf_columns},
	},
};

/// PatchNode merges assignment values with original row values.
/// Unlike ExtendNode which adds new columns, PatchNode replaces
/// columns that have matching names in the assignments.
pub(crate) struct PatchNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	udf_names: Vec<String>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl PatchNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			udf_names: Vec::new(),
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for PatchNode {
	#[instrument(name = "volcano::patch::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let (input, expressions, udf_names) = UdfEvalNode::wrap_if_needed(
			mem::replace(&mut self.input, Box::new(NoopNode)),
			&self.expressions,
			&ctx.symbols,
		);
		self.input = input;
		self.expressions = expressions;
		self.udf_names = udf_names;

		let compile_ctx = CompileContext {			symbols: &ctx.symbols,
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

		if let Some(columns) = self.input.next(rx, ctx)? {
			let stored_ctx = &self.context.as_ref().unwrap().0;
			let transform_ctx = TransformContext {			routines: &ctx.services.routines,
				runtime_context: &stored_ctx.services.runtime_context,
				params: &stored_ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;

			if self.headers.is_none() {
				let result_headers: Vec<Fragment> = result.iter().map(|c| c.name().clone()).collect();
				self.headers = Some(ColumnHeaders {
					columns: result_headers,
				});
			}

			let mut result = result;
			strip_udf_columns(&mut result, &self.udf_names);
			Ok(Some(result))
		} else {
			Ok(None)
		}
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
		let created_at = input.created_at.clone();
		let updated_at = input.updated_at.clone();

		let patch_names: Vec<Fragment> = self.expressions.iter().map(column_name_from_expression).collect();

		let session = EvalContext::from_transform(ctx, stored_ctx);
		let mut patch_columns = Vec::with_capacity(self.expressions.len());
		for (expr, compiled_expr) in self.expressions.iter().zip(compiled.iter()) {
			let mut exec_ctx = session.with_eval(input.clone(), row_count);

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

			if let Some(target_type) = exec_ctx.target.as_ref().map(|t| t.column_type())
				&& column.data.get_type() != target_type
			{
				let data =
					cast_column_data(&exec_ctx, &column.data, target_type, &expr.lazy_fragment())?;
				column = ColumnWithName {
					name: column.name,
					data,
				};
			}

			patch_columns.push(column);
		}

		let mut result_columns: Vec<ColumnWithName> = Vec::new();

		for (original_name, original_data) in input.names.iter().zip(input.columns.iter()) {
			let original_name_text = original_name.text();

			if let Some(patch_idx) = patch_names.iter().position(|n| n.text() == original_name_text) {
				result_columns.push(patch_columns[patch_idx].clone());
			} else {
				result_columns.push(ColumnWithName::new(original_name.clone(), original_data.clone()));
			}
		}

		for (patch_idx, patch_name) in patch_names.iter().enumerate() {
			if !result_columns.iter().any(|c| c.name().text() == patch_name.text()) {
				result_columns.push(patch_columns[patch_idx].clone());
			}
		}

		let mut names_vec = Vec::with_capacity(result_columns.len());
		let mut buffers_vec = Vec::with_capacity(result_columns.len());
		for c in result_columns {
			names_vec.push(c.name);
			buffers_vec.push(c.data);
		}
		Ok(Columns {
			row_numbers: CowVec::new(row_numbers),
			created_at,
			updated_at,
			columns: CowVec::new(buffers_vec),
			names: CowVec::new(names_vec),
		})
	}
}
