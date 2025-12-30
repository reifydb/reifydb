// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	pin::Pin,
	task::{Context, Poll},
};

use futures_util::Stream;
use pin_project::pin_project;
use reifydb_core::value::column::Columns;

use crate::{
	error::Result,
	expr::{CompiledFilter, EvalContext},
	pipeline::Pipeline,
};

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
		Box::pin(FilterStream {
			input,
			predicate: self.predicate.clone(),
			eval_ctx: self.eval_ctx.clone(),
		})
	}
}

#[pin_project]
struct FilterStream {
	#[pin]
	input: Pipeline,
	predicate: CompiledFilter,
	eval_ctx: EvalContext,
}

impl Stream for FilterStream {
	type Item = Result<Columns>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();

		loop {
			match this.input.as_mut().poll_next(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => return Poll::Ready(None),
				Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
				Poll::Ready(Some(Ok(batch))) => {
					// Evaluate compiled filter to get filter mask
					let mask = match this.predicate.eval(&batch, this.eval_ctx) {
						Ok(m) => m,
						Err(e) => return Poll::Ready(Some(Err(e))),
					};

					// Check if any rows pass
					if mask.none() {
						// All rows filtered out, continue to next batch
						continue;
					}

					// Check if all rows pass
					if mask.count_ones() == batch.row_count() {
						// All rows pass, return unchanged
						return Poll::Ready(Some(Ok(batch)));
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

					let filtered = batch.extract_by_indices(&indices);
					return Poll::Ready(Some(Ok(filtered)));
				}
			}
		}
	}
}
