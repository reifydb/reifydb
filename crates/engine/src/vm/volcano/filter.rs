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
use reifydb_type::util::bitvec::BitVec;
use tracing::instrument;

use crate::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
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
			if let Some(mut columns) = self.input.next(rx, ctx)? {
				let mut row_count = columns.row_count();

				// Apply each filter expression sequentially
				for compiled_expr in compiled {
					// Early exit if no rows remain
					if row_count == 0 {
						break;
					}

					// Create execution context for all current rows
					let exec_ctx = EvalContext {
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

					// Execute the compiled filter expression
					let result = compiled_expr.execute(&exec_ctx)?;

					// Create filter mask from result
					let filter_mask = match result.data() {
						ColumnData::Bool(container) => {
							let mut mask = BitVec::repeat(row_count, false);
							for i in 0..row_count {
								if i < container.data().len()
									&& i < container.bitvec().len()
								{
									let valid = container.is_defined(i);
									let filter_result = container.data().get(i);
									mask.set(i, valid & filter_result);
								}
							}
							mask
						}
						ColumnData::Undefined(_) => BitVec::repeat(row_count, false),
						_ => panic!("filter expression must column to a boolean column"),
					};

					columns.filter(&filter_mask)?;
					row_count = columns.row_count();
				}

				if row_count > 0 {
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
			};

			let result = compiled_expr.execute(&exec_ctx)?;

			// Extract mask from boolean column result
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
				ColumnData::Undefined(_) => {
					for i in 0..row_count {
						mask.set(i, false);
					}
				}
				_ => panic!("filter expression must evaluate to a boolean column"),
			}
		}

		Ok(Some(mask))
	}
}
