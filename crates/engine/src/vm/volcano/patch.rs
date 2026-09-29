// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	expression::{Expression, name::display_label},
	interface::{
		evaluate::TargetColumn,
		resolved::{ResolvedColumn, ResolvedObject},
	},
	value::column::{headers::ColumnHeaders, write::check_digest_write},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
	eval::cast_for_write,
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{column_view::ColumnView, system_columns::user_columns},
};
use tracing::instrument;

use super::{NoopNode, user_header_names, with_system_headers, with_user_columns};
use crate::{
	Result,
	vm::volcano::{
		inline::{expand_aliases, expand_sumtype_assignment, resolve_is_variants},
		query::{QueryContext, QueryNode, eval_context_from_transform},
		udf::{UdfEvalNode, strip_udf_columns},
	},
};

pub(crate) struct PatchNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	source: Option<ResolvedObject>,
	udf_names: Vec<String>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl PatchNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>, source: Option<ResolvedObject>) -> Self {
		Self {
			input,
			expressions,
			source,
			udf_names: Vec::new(),
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for PatchNode {
	#[instrument(name = "volcano::patch::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		if let Some(source) = ctx.source.as_ref().or(self.source.as_ref()) {
			(self.expressions, _) =
				expand_aliases(mem::take(&mut self.expressions), |alias_expr, expanded| {
					expand_sumtype_assignment(ctx, rx, source, alias_expr, expanded)
				})?;
		}
		if let Some(source) = self.source.as_ref() {
			for expr in &mut self.expressions {
				resolve_is_variants(&ctx.services.catalog, rx, source, expr)?;
			}
		}
		let (input, expressions, udf_names) = UdfEvalNode::wrap_if_needed(
			mem::replace(&mut self.input, Box::new(NoopNode)),
			&self.expressions,
			&ctx.symbols,
		);
		self.input = input;
		self.expressions = expressions;
		self.udf_names = udf_names;

		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let compiled = self
			.expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()?;
		self.context = Some((Arc::new(ctx.clone()), compiled));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "volcano::patch::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "PatchNode::next() called before initialize()");
		}

		if let Some(columns) = self.input.next(rx, ctx)? {
			let stored_ctx = &self.context.as_ref().unwrap().0;
			let transform_ctx = TransformContext {
				routines: &ctx.services.routines,
				runtime_context: &stored_ctx.services.runtime_context,
				params: &stored_ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;

			if self.headers.is_none() {
				self.headers = Some(ColumnHeaders::from_batch(&result));
			}

			let result = strip_udf_columns(result, &self.udf_names)?;
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
		let patch_names: Vec<Fragment> = self.expressions.iter().map(display_label).collect();

		let mut result = Vec::new();
		for col in &user_header_names(&input_headers) {
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

		Some(with_system_headers(result, &input_headers))
	}
}

impl Transform for PatchNode {
	fn apply(&self, ctx: &TransformContext, input: RecordBatch) -> Result<RecordBatch> {
		let (stored_ctx, compiled) =
			self.context.as_ref().expect("PatchNode::apply() called before initialize()");

		let row_count = input.num_rows();

		let patch_names: Vec<Fragment> = self.expressions.iter().map(display_label).collect();

		let session = eval_context_from_transform(ctx, stored_ctx);
		let mut patch_columns = Vec::with_capacity(self.expressions.len());
		for (expr, compiled_expr) in self.expressions.iter().zip(compiled.iter()) {
			let mut exec_ctx = Self::eval_context(&session, &input, row_count);

			if let (Expression::Alias(alias_expr), Some(source)) = (expr, &stored_ctx.source) {
				let alias_name = alias_expr.alias.name();

				if let Some(table_column) = source.columns().iter().find(|col| col.name == alias_name) {
					let column_ident = Fragment::internal(&table_column.name);
					let resolved_column =
						ResolvedColumn::new(column_ident, source.clone(), table_column.clone());
					exec_ctx.target = Some(TargetColumn::Resolved(resolved_column));
				}
			}

			let mut column = Self::eval_patch(compiled_expr, &exec_ctx)?;

			if let Some(target_type) = exec_ctx.target.as_ref().map(|t| t.column_type()) {
				let view = ColumnView::try_from(&column)?;
				if view.get_type() != target_type {
					check_digest_write(&view, &target_type, expr.lazy_fragment())?;
					column = cast_for_write(&exec_ctx, &view, target_type, &expr.lazy_fragment())?;
				}
			}

			patch_columns.push(column);
		}

		Self::merge(input, &patch_names, patch_columns)
	}
}

impl PatchNode {
	#[instrument(level = "trace", skip_all, name = "volcano::patch::eval_context")]
	fn eval_context<'e>(session: &EvalContext<'e>, input: &RecordBatch, row_count: usize) -> EvalContext<'e> {
		session.with_eval(input.clone(), row_count)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::patch::eval")]
	fn eval_patch(compiled: &CompiledExpr, exec_ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
		compiled.execute(exec_ctx)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::patch::merge")]
	fn merge(
		input: RecordBatch,
		patch_names: &[Fragment],
		patch_columns: Vec<(FieldRef, ArrayRef)>,
	) -> Result<RecordBatch> {
		let mut result_columns: Vec<(FieldRef, ArrayRef)> = Vec::new();

		for (original_field, original_data) in user_columns(&input) {
			let original_name_text = original_field.name().as_str();

			if let Some(patch_idx) = patch_names.iter().position(|n| n.text() == original_name_text) {
				result_columns.push(patch_columns[patch_idx].clone());
			} else {
				result_columns.push((original_field.clone(), original_data.clone()));
			}
		}

		for (patch_idx, patch_name) in patch_names.iter().enumerate() {
			if !result_columns.iter().any(|(field, _)| field.name() == patch_name.text()) {
				result_columns.push(patch_columns[patch_idx].clone());
			}
		}

		with_user_columns(result_columns, &input)
	}
}
