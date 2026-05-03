// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::{catalog::dictionary::Dictionary, resolved::ResolvedShape},
	value::{
		batch::lazy::LazyBatch,
		column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
	},
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_rql::expression::{Expression, name::display_label};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{util::bitvec::BitVec, value::constraint::Constraint};
use tracing::instrument;

use super::{NoopNode, decode_dictionary_columns};
use crate::{
	Result,
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::volcano::{
		query::{QueryContext, QueryNode},
		udf::{UdfEvalNode, strip_udf_columns},
	},
};

pub(crate) struct FilterNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	udf_names: Vec<String>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl FilterNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			udf_names: Vec::new(),
			context: None,
		}
	}
}

impl QueryNode for FilterNode {
	#[instrument(level = "trace", skip_all, name = "volcano::filter::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
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
			.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
			.collect();
		self.context = Some((Arc::new(ctx.clone()), compiled));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::filter::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "FilterNode::next() called before initialize()");
		let (stored_ctx, compiled) = self.context.as_ref().unwrap();

		loop {
			if let Some(mut lazy_batch) = self.input.next_lazy(rx, ctx)? {
				let filter_result =
					self.evaluate_filter_on_lazy(&lazy_batch, stored_ctx, compiled, rx)?;

				if let Some(filter_mask) = filter_result {
					lazy_batch.apply_filter(&filter_mask);
				}

				if lazy_batch.valid_row_count() == 0 {
					continue;
				}

				let dictionaries: Vec<Option<Dictionary>> =
					lazy_batch.column_metas().iter().map(|m| m.dictionary.clone()).collect();

				let mut columns = lazy_batch.into_columns();

				decode_dictionary_columns(&mut columns, &dictionaries, rx)?;

				strip_udf_columns(&mut columns, &self.udf_names);
				return Ok(Some(columns));
			}

			if let Some(columns) = self.input.next(rx, ctx)? {
				let transform_ctx = TransformContext {
					routines: &ctx.services.routines,
					runtime_context: &stored_ctx.services.runtime_context,
					params: &stored_ctx.params,
				};
				let mut columns = self.apply(&transform_ctx, columns)?;
				if columns.row_count() > 0 {
					strip_udf_columns(&mut columns, &self.udf_names);
					return Ok(Some(columns));
				}
			} else {
				return Ok(None);
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl Transform for FilterNode {
	fn apply(&self, ctx: &TransformContext, input: Columns) -> Result<Columns> {
		let (stored_ctx, compiled) =
			self.context.as_ref().expect("FilterNode::apply() called before initialize()");

		let session = EvalContext::from_transform(ctx, stored_ctx);
		let mut columns = input;
		let mut row_count = columns.row_count();

		for compiled_expr in compiled {
			if row_count == 0 {
				break;
			}

			let exec_ctx = session.with_eval(columns.clone(), row_count);

			let result = compiled_expr.execute(&exec_ctx)?;

			let filter_mask = match result.data() {
				ColumnBuffer::Bool(container) => {
					let mut mask = BitVec::repeat(row_count, false);
					for i in 0..row_count {
						if i < container.len() {
							let valid = container.is_defined(i);
							let filter_result = container.data().get(i);
							mask.set(i, valid & filter_result);
						}
					}
					mask
				}
				ColumnBuffer::Option {
					inner,
					bitvec,
				} => match inner.as_ref() {
					ColumnBuffer::Bool(container) => {
						let mut mask = BitVec::repeat(row_count, false);
						for i in 0..row_count {
							let defined = i < bitvec.len() && bitvec.get(i);
							let valid = defined && container.is_defined(i);
							let value = valid && container.data().get(i);
							mask.set(i, value);
						}
						mask
					}
					_ => panic!("filter expression must evaluate to a boolean column"),
				},
				_ => panic!("filter expression must evaluate to a boolean column"),
			};

			columns.filter(&filter_mask)?;
			row_count = columns.row_count();
		}

		Ok(columns)
	}
}

impl FilterNode {
	fn evaluate_filter_on_lazy<'a>(
		&self,
		lazy_batch: &LazyBatch,
		ctx: &QueryContext,
		compiled: &[CompiledExpr],
		rx: &mut Transaction<'a>,
	) -> Result<Option<BitVec>> {
		let dictionaries: Vec<Option<Dictionary>> =
			lazy_batch.column_metas().iter().map(|m| m.dictionary.clone()).collect();
		let mut columns = lazy_batch.clone().into_columns();
		decode_dictionary_columns(&mut columns, &dictionaries, rx)?;
		let row_count = columns.row_count();

		if row_count == 0 {
			return Ok(Some(BitVec::empty()));
		}

		let session = EvalContext::from_query(ctx);
		let mut mask = BitVec::repeat(row_count, true);

		for compiled_expr in compiled {
			let exec_ctx = session.with_eval(columns.clone(), row_count);

			let result = compiled_expr.execute(&exec_ctx)?;

			match result.data() {
				ColumnBuffer::Bool(container) => {
					for i in 0..row_count {
						if mask.get(i) {
							let valid = container.is_defined(i);
							let filter_result = container.data().get(i);
							mask.set(i, valid & filter_result);
						}
					}
				}
				ColumnBuffer::Option {
					inner,
					bitvec,
				} => match inner.as_ref() {
					ColumnBuffer::Bool(container) => {
						for i in 0..row_count {
							if mask.get(i) {
								let defined = i < bitvec.len() && bitvec.get(i);
								let valid = defined && container.is_defined(i);
								let value = valid && container.data().get(i);
								mask.set(i, value);
							}
						}
					}
					_ => panic!("filter expression must evaluate to a boolean column"),
				},
				_ => panic!("filter expression must evaluate to a boolean column"),
			}
		}

		Ok(Some(mask))
	}
}

pub(crate) fn resolve_is_variant_tags(
	expr: &mut Expression,
	source: &ResolvedShape,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
) -> Result<()> {
	match expr {
		Expression::IsVariant(e) => {
			let col_name = match e.expression.as_ref() {
				Expression::Column(c) => c.0.name.text().to_string(),
				other => display_label(other).text().to_string(),
			};

			let tag_col_name = format!("{}_tag", col_name);
			let columns = source.columns();
			if let Some(tag_col) = columns.iter().find(|c| c.name == tag_col_name)
				&& let Some(Constraint::SumType(id)) = tag_col.constraint.constraint()
			{
				let def = catalog.get_sumtype(rx, *id)?;
				let variant_name = e.variant_name.text().to_lowercase();
				if let Some(variant) =
					def.variants.iter().find(|v| v.name.to_lowercase() == variant_name)
				{
					e.tag = Some(variant.tag);
				}
			}
			resolve_is_variant_tags(&mut e.expression, source, catalog, rx)?;
		}
		Expression::And(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::Or(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::Equal(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::NotEqual(e) => {
			resolve_is_variant_tags(&mut e.left, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.right, source, catalog, rx)?;
		}
		Expression::Prefix(e) => {
			resolve_is_variant_tags(&mut e.expression, source, catalog, rx)?;
		}
		Expression::If(e) => {
			resolve_is_variant_tags(&mut e.condition, source, catalog, rx)?;
			resolve_is_variant_tags(&mut e.then_expr, source, catalog, rx)?;
			for else_if in &mut e.else_ifs {
				resolve_is_variant_tags(&mut else_if.condition, source, catalog, rx)?;
				resolve_is_variant_tags(&mut else_if.then_expr, source, catalog, rx)?;
			}
			if let Some(else_expr) = &mut e.else_expr {
				resolve_is_variant_tags(else_expr, source, catalog, rx)?;
			}
		}
		Expression::Alias(e) => {
			resolve_is_variant_tags(&mut e.expression, source, catalog, rx)?;
		}
		_ => {}
	}
	Ok(())
}
