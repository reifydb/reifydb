// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::dictionary::DictionaryDef,
	value::{
		batch::lazy::LazyBatch,
		column::{columns::Columns, data::ColumnData, headers::ColumnHeaders},
	},
};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{util::bitvec::BitVec, value::constraint::Constraint};
use tracing::instrument;

use crate::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	transform::{Transform, context::TransformContext},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct FilterNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl FilterNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			context: None,
		}
	}
}

impl QueryNode for FilterNode {
	#[instrument(level = "trace", skip_all, name = "volcano::filter::initialize")]
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

	#[instrument(level = "trace", skip_all, name = "volcano::filter::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "FilterNode::next() called before initialize()");
		let (stored_ctx, compiled) = self.context.as_ref().unwrap();

		loop {
			// Try lazy path first
			if let Some(mut lazy_batch) = self.input.next_lazy(rx, ctx)? {
				// Evaluate filter on lazy batch
				let filter_result =
					self.evaluate_filter_on_lazy(&lazy_batch, stored_ctx, compiled, rx)?;

				if let Some(filter_mask) = filter_result {
					lazy_batch.apply_filter(&filter_mask);
				}

				if lazy_batch.valid_row_count() == 0 {
					continue; // Skip to next batch
				}

				// Save dictionary metadata before consuming the lazy batch
				let dictionaries: Vec<Option<DictionaryDef>> =
					lazy_batch.column_metas().iter().map(|m| m.dictionary.clone()).collect();

				// Materialize surviving rows
				let mut columns = lazy_batch.into_columns();

				// Decode dictionary columns back to actual values
				super::decode_dictionary_columns(&mut columns, &dictionaries, rx)?;

				return Ok(Some(columns));
			}

			// Fall back to materialized path
			if let Some(columns) = self.input.next(rx, ctx)? {
				let transform_ctx = TransformContext {
					functions: &stored_ctx.services.functions,
					clock: &stored_ctx.services.clock,
					params: &stored_ctx.params,
				};
				let columns = self.apply(&transform_ctx, columns)?;
				if columns.row_count() > 0 {
					return Ok(Some(columns));
				}
			} else {
				// No more batches
				return Ok(None);
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl Transform for FilterNode {
	fn apply(&self, ctx: &TransformContext, input: Columns) -> reifydb_type::Result<Columns> {
		let (stored_ctx, compiled) =
			self.context.as_ref().expect("FilterNode::apply() called before initialize()");

		let mut columns = input;
		let mut row_count = columns.row_count();

		for compiled_expr in compiled {
			if row_count == 0 {
				break;
			}

			let exec_ctx = EvalContext {
				target: None,
				columns: columns.clone(),
				row_count,
				take: None,
				params: ctx.params,
				symbol_table: &stored_ctx.stack,
				is_aggregate_context: false,
				functions: ctx.functions,
				clock: ctx.clock,
				arena: None,
			};

			let result = compiled_expr.execute(&exec_ctx)?;

			let filter_mask = match result.data() {
				ColumnData::Bool(container) => {
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
				ColumnData::Option {
					inner,
					bitvec,
				} => match inner.as_ref() {
					ColumnData::Bool(container) => {
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
	/// Evaluate filter expressions on a lazy batch using column-oriented evaluation.
	/// Returns a filter mask indicating which rows pass all filter expressions.
	fn evaluate_filter_on_lazy<'a>(
		&self,
		lazy_batch: &LazyBatch,
		ctx: &QueryContext,
		compiled: &[CompiledExpr],
		rx: &mut Transaction<'a>,
	) -> crate::Result<Option<BitVec>> {
		// Materialize to columns for column-oriented evaluation,
		// then decode dictionary columns so filters can compare actual values.
		let dictionaries: Vec<Option<DictionaryDef>> =
			lazy_batch.column_metas().iter().map(|m| m.dictionary.clone()).collect();
		let mut columns = lazy_batch.clone().into_columns();
		super::decode_dictionary_columns(&mut columns, &dictionaries, rx)?;
		let row_count = columns.row_count();

		if row_count == 0 {
			return Ok(Some(BitVec::empty()));
		}

		let mut mask = BitVec::repeat(row_count, true);

		for compiled_expr in compiled {
			let exec_ctx = EvalContext {
				target: None,
				columns: columns.clone(),
				row_count,
				take: None,
				params: &ctx.params,
				symbol_table: &ctx.stack,
				is_aggregate_context: false,
				functions: &ctx.services.functions,
				clock: &ctx.services.clock,
				arena: None,
			};

			let result = compiled_expr.execute(&exec_ctx)?;

			match result.data() {
				ColumnData::Bool(container) => {
					for i in 0..row_count {
						if mask.get(i) {
							let valid = container.is_defined(i);
							let filter_result = container.data().get(i);
							mask.set(i, valid & filter_result);
						}
					}
				}
				ColumnData::Option {
					inner,
					bitvec,
				} => match inner.as_ref() {
					ColumnData::Bool(container) => {
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
	source: &reifydb_core::interface::resolved::ResolvedPrimitive,
	catalog: &reifydb_catalog::catalog::Catalog,
	rx: &mut Transaction<'_>,
) -> crate::Result<()> {
	match expr {
		Expression::IsVariant(e) => {
			let col_name = match e.expression.as_ref() {
				Expression::Column(c) => c.0.name.text().to_string(),
				other => other.full_fragment_owned().text().to_string(),
			};

			let tag_col_name = format!("{}_tag", col_name);
			let columns = source.columns();
			if let Some(tag_col) = columns.iter().find(|c| c.name == tag_col_name) {
				if let Some(Constraint::SumType(id)) = tag_col.constraint.constraint() {
					let def = catalog.get_sumtype(rx, *id)?;
					let variant_name = e.variant_name.text().to_lowercase();
					if let Some(variant) =
						def.variants.iter().find(|v| v.name.to_lowercase() == variant_name)
					{
						e.tag = Some(variant.tag);
					}
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
