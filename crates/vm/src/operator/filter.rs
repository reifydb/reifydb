// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::batch::Batch;
use reifydb_rqlv2::expression::{eval::context::EvalContext, types::CompiledFilter};

use crate::{error::Result, pipeline::Pipeline};

/// Filter operator - applies a compiled boolean predicate to filter rows.
pub struct FilterOp {
	pub predicate: CompiledFilter,
	pub eval_ctx: EvalContext,
}

impl FilterOp {
	/// Create a new filter operator with a compiled filter.
	pub fn new(predicate: CompiledFilter) -> Self {
		Self {
			predicate,
			eval_ctx: EvalContext::new(),
		}
	}

	/// Create a new filter operator with a compiled filter and evaluation context.
	pub fn with_context(predicate: CompiledFilter, eval_ctx: EvalContext) -> Self {
		Self {
			predicate,
			eval_ctx,
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		Box::new(FilterIterator {
			input,
			predicate: self.predicate.clone(),
			eval_ctx: self.eval_ctx.clone(),
		})
	}
}

/// Iterator that filters batches based on a predicate
struct FilterIterator {
	input: Pipeline,
	predicate: CompiledFilter,
	eval_ctx: EvalContext,
}

impl Iterator for FilterIterator {
	type Item = Result<Batch>;

	fn next(&mut self) -> Option<Self::Item> {
		// Keep trying batches until we find one with rows that pass
		loop {
			let batch = match self.input.next()? {
				Ok(b) => b,
				Err(e) => return Some(Err(e)),
			};

			// Materialize batch for filter evaluation
			let columns = batch.clone().into_columns();

			// Evaluate compiled filter to get filter mask
			let mask = match self.predicate.eval(&columns, &self.eval_ctx) {
				Ok(m) => m,
				Err(e) => return Some(Err(e.into())),
			};

			// Check if any rows pass
			if mask.none() {
				// All rows filtered out, try next batch
				continue;
			}

			// Check if all rows pass
			if mask.count_ones() == batch.row_count() {
				// All rows pass, return unchanged
				return Some(Ok(batch));
			}

			// Apply filter - collect indices where mask is true
			let indices: Vec<usize> = mask
				.iter()
				.enumerate()
				.filter_map(|(i, b)| {
					if b {
						Some(i)
					} else {
						None
					}
				})
				.collect();

			return Some(Ok(batch.extract_by_indices(&indices)));
		}
	}
}
