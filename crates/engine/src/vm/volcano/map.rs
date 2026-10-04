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
	value::{batch::batch, column::headers::ColumnHeaders},
};
use reifydb_evaluate::{
	expression::{context::EvalContext, eval::cast_for_write},
	lower::LoweredExpr,
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, reifydb_assertions, value::column_view::ColumnView};
use tracing::instrument;

use super::{NoopNode, with_system_headers, with_user_columns};
use crate::{
	Result,
	vm::volcano::{
		inline::{expand_aliases, expand_sumtype_ctor, reject_variant_column_clashes, resolve_is_variants},
		query::{QueryContext, QueryNode, eval_context_from_query, eval_context_from_transform},
		udf::{UdfEvalNode, evaluate_udfs_no_input, strip_udf_columns},
	},
};

pub(crate) struct MapNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	source: Option<ResolvedObject>,
	udf_names: Vec<String>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<LoweredExpr>)>,
}

impl MapNode {
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

impl QueryNode for MapNode {
	#[instrument(name = "volcano::map::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let (expressions, written) =
			expand_aliases(mem::take(&mut self.expressions), |alias_expr, expanded| {
				expand_sumtype_ctor(ctx, rx, ctx.source.as_ref(), alias_expr, expanded)
			})?;
		reject_variant_column_clashes(&expressions.iter().map(display_label).collect::<Vec<_>>(), &written)?;
		self.expressions = expressions;
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

		let lowered = self.expressions.iter().map(|e| LoweredExpr::new(e.clone(), "map")).collect();
		self.context = Some((Arc::new(ctx.clone()), lowered));
		self.input.initialize(rx, ctx)?;
		let column_names = self.expressions.iter().map(display_label).collect();
		self.headers = Some(match self.input.headers() {
			Some(input_headers) => with_system_headers(column_names, &input_headers),
			None => ColumnHeaders {
				columns: column_names,
			},
		});
		Ok(())
	}

	#[instrument(name = "volcano::map::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "MapNode::next() called before initialize()");
		}

		if let Some(columns) = self.input.next(rx, ctx)? {
			let stored_ctx = &self.context.as_ref().unwrap().0;
			let transform_ctx = TransformContext {
				routines: &ctx.services.routines,
				runtime_context: &stored_ctx.services.runtime_context,
				params: &stored_ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;
			let result = strip_udf_columns(result, &self.udf_names)?;

			Ok(Some(result))
		} else {
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

impl Transform for MapNode {
	fn apply(&self, ctx: &TransformContext, input: RecordBatch) -> Result<RecordBatch> {
		let (stored_ctx, lowered) = self.context.as_ref().expect("MapNode::apply() called before initialize()");

		let row_count = input.num_rows();
		let session = eval_context_from_transform(ctx, stored_ctx);
		let mut new_columns = Vec::with_capacity(lowered.len());

		for (expr, lowered_expr) in self.expressions.iter().zip(lowered.iter()) {
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

			let mut column = Self::eval_projection(lowered_expr, &exec_ctx)?;

			if let Some(target_type) = exec_ctx.target.as_ref().map(|t| t.column_type()) {
				let view = ColumnView::try_from(&column)?;
				if view.get_type() != target_type {
					column = cast_for_write(&exec_ctx, &view, target_type, &expr.lazy_fragment())?;
				}
			}

			new_columns.push(column);
		}

		Self::assemble(&input, new_columns)
	}
}

impl MapNode {
	#[instrument(level = "trace", skip_all, name = "volcano::map::eval_context")]
	fn eval_context<'e>(session: &EvalContext<'e>, input: &RecordBatch, row_count: usize) -> EvalContext<'e> {
		session.with_eval(input.clone(), row_count)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::map::eval")]
	fn eval_projection(lowered: &LoweredExpr, exec_ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
		lowered.evaluate(exec_ctx)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::map::assemble")]
	fn assemble(input: &RecordBatch, new_columns: Vec<(FieldRef, ArrayRef)>) -> Result<RecordBatch> {
		with_user_columns(new_columns, input)
	}
}

pub(crate) struct MapWithoutInputNode {
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,

	udf_columns: Option<RecordBatch>,
	context: Option<(Arc<QueryContext>, Vec<LoweredExpr>)>,
}

impl MapWithoutInputNode {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			headers: None,
			udf_columns: None,
			context: None,
		}
	}
}

impl QueryNode for MapWithoutInputNode {
	#[instrument(name = "volcano::map::noinput::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let (expressions, written) =
			expand_aliases(mem::take(&mut self.expressions), |alias_expr, expanded| {
				expand_sumtype_ctor(ctx, rx, ctx.source.as_ref(), alias_expr, expanded)
			})?;
		reject_variant_column_clashes(&expressions.iter().map(display_label).collect::<Vec<_>>(), &written)?;
		self.expressions = expressions;
		if let Some((rewritten, udf_cols)) = evaluate_udfs_no_input(&self.expressions, ctx, rx)? {
			self.expressions = rewritten;
			self.udf_columns = Some(udf_cols);
		}

		let lowered = self.expressions.iter().map(|e| LoweredExpr::new(e.clone(), "map")).collect();
		self.context = Some((Arc::new(ctx.clone()), lowered));
		Ok(())
	}

	#[instrument(name = "volcano::map::noinput::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "MapWithoutInputNode::next() called before initialize()");
		}
		let (stored_ctx, lowered) = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let session = eval_context_from_query(stored_ctx);
		let mut columns = vec![];

		for lowered_expr in lowered {
			let exec_ctx = match &self.udf_columns {
				Some(udf_cols) => session.with_eval(udf_cols.clone(), 1),
				None => session.with_eval_empty(),
			};

			let column = lowered_expr.evaluate(&exec_ctx)?;

			columns.push(column);
		}

		let columns = batch(columns)?;
		self.headers = Some(ColumnHeaders::from_batch(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
