// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::{
	batch::lazy::{LazyBatch, LazyColumnMeta},
	column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	util::bitvec::BitVec,
	value::{Value, dictionary::DictionaryEntryId},
};
use tracing::instrument;

use crate::{
	evaluate::{ColumnEvaluationContext, column::evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
	transaction::operation::dictionary::DictionaryOperations,
};

pub(crate) struct FilterNode {
	input: Box<ExecutionPlan>,
	expressions: Vec<Expression>,
	context: Option<Arc<ExecutionContext>>,
}

impl FilterNode {
	pub fn new(input: Box<ExecutionPlan>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			context: None,
		}
	}
}

impl QueryNode for FilterNode {
	#[instrument(level = "trace", skip_all, name = "query::filter::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "query::filter::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "FilterNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		loop {
			// Try lazy path first
			if let Some(mut lazy_batch) = self.input.next_lazy(rx, ctx)? {
				// Save column_metas before lazy_batch is consumed
				let column_metas: Vec<_> = lazy_batch.column_metas().to_vec();

				// Evaluate filter on lazy batch (decodes dictionaries for comparison)
				let filter_result = self.evaluate_filter_on_lazy(&lazy_batch, stored_ctx, rx)?;

				if let Some(filter_mask) = filter_result {
					lazy_batch.apply_filter(&filter_mask);
				}

				if lazy_batch.valid_row_count() == 0 {
					continue; // Skip to next batch
				}

				// Materialize only surviving rows
				let mut columns = lazy_batch.into_columns();

				// Decode dictionary columns for output
				Self::decode_dictionary_columns(&mut columns, &column_metas, rx)?;

				return Ok(Some(Batch {
					columns,
				}));
			}

			// Fall back to materialized path
			if let Some(Batch {
				mut columns,
			}) = self.input.next(rx, ctx)?
			{
				let mut row_count = columns.row_count();

				// Apply each filter expression sequentially
				for filter_expr in &self.expressions {
					// Early exit if no rows remain
					if row_count == 0 {
						break;
					}

					// Create evaluation context for all current rows
					let eval_ctx = ColumnEvaluationContext {
						target: None,
						columns: columns.clone(),
						row_count,
						take: None,
						params: &stored_ctx.params,
						stack: &stored_ctx.stack,
						is_aggregate_context: false,
					};

					// Evaluate the filter expression
					let result = evaluate(&eval_ctx, filter_expr)?;

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
						_ => panic!("filter expression must column to a boolean column"),
					};

					columns.filter(&filter_mask)?;
					row_count = columns.row_count();
				}

				if row_count > 0 {
					return Ok(Some(Batch {
						columns,
					}));
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
	/// Decode dictionary columns by replacing dictionary IDs with actual values.
	fn decode_dictionary_columns<'a>(
		columns: &mut Columns,
		column_metas: &[LazyColumnMeta],
		rx: &mut Transaction<'a>,
	) -> crate::Result<()> {
		for (col_idx, meta) in column_metas.iter().enumerate() {
			if let Some(dictionary) = &meta.dictionary {
				let col = &columns[col_idx];
				let row_count = col.data().len();

				let mut new_data = ColumnData::with_capacity(dictionary.value_type, row_count);

				for row_idx in 0..row_count {
					let id_value = col.data().get_value(row_idx);
					if let Some(entry_id) = DictionaryEntryId::from_value(&id_value) {
						if let Some(decoded) = rx.get_from_dictionary(dictionary, entry_id)? {
							new_data.push_value(decoded);
						} else {
							new_data.push_value(Value::Undefined);
						}
					} else {
						new_data.push_value(Value::Undefined);
					}
				}

				let col_name = columns[col_idx].name().clone();
				columns.columns.make_mut()[col_idx] = Column {
					name: col_name,
					data: new_data,
				};
			}
		}
		Ok(())
	}

	/// Evaluate filter expressions on a lazy batch using column-oriented evaluation.
	/// Returns a filter mask indicating which rows pass all filter expressions.
	fn evaluate_filter_on_lazy<'a>(
		&self,
		lazy_batch: &LazyBatch,
		ctx: &ExecutionContext,
		rx: &mut Transaction<'a>,
	) -> crate::Result<Option<BitVec>> {
		// Materialize to columns for column-oriented evaluation
		let mut columns = lazy_batch.clone().into_columns();
		let row_count = columns.row_count();

		if row_count == 0 {
			return Ok(Some(BitVec::empty()));
		}

		// Decode dictionaries BEFORE filter evaluation so comparisons work correctly
		Self::decode_dictionary_columns(&mut columns, lazy_batch.column_metas(), rx)?;

		let mut mask = BitVec::repeat(row_count, true);

		for filter_expr in &self.expressions {
			// Use the existing column evaluator
			let eval_ctx = ColumnEvaluationContext {
				target: None,
				columns: columns.clone(),
				row_count,
				take: None,
				params: &ctx.params,
				stack: &ctx.stack,
				is_aggregate_context: false,
			};

			let result = evaluate(&eval_ctx, filter_expr)?;

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
				_ => panic!("filter expression must evaluate to a boolean column"),
			}
		}

		Ok(Some(mask))
	}
}
