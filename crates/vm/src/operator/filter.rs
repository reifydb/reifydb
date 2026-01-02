// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_rqlv2::expression::{CompiledFilter, EvalContext};

use crate::pipeline::Pipeline;

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
		let predicate = self.predicate.clone();
		let eval_ctx = self.eval_ctx.clone();

		Box::pin(input.filter_map(move |result| {
			let predicate = predicate.clone();
			let eval_ctx = eval_ctx.clone();
			async move {
				let batch = match result {
					Err(e) => return Some(Err(e)),
					Ok(b) => b,
				};

				// Evaluate compiled filter to get filter mask
				let mask = match predicate.eval(&batch, &eval_ctx).await {
					Err(e) => return Some(Err(e.into())), // Convert EvalError to VmError
					Ok(m) => m,
				};

				// Check if any rows pass
				if mask.none() {
					// All rows filtered out, skip this batch
					return None;
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

				Some(Ok(batch.extract_by_indices(&indices)))
			}
		}))
	}
}
