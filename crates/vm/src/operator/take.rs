// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::{StreamExt, stream::unfold};

use crate::pipeline::Pipeline;

/// Take operator - limits the number of rows returned.
pub struct TakeOp {
	pub limit: usize,
}

impl TakeOp {
	pub fn new(limit: usize) -> Self {
		Self {
			limit,
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		let limit = self.limit;

		Box::pin(unfold((input, limit), |(mut input, remaining)| async move {
			if remaining == 0 {
				return None;
			}

			match input.next().await? {
				Err(e) => Some((Err(e), (input, 0))),
				Ok(batch) => {
					let batch_size = batch.row_count();

					if batch_size <= remaining {
						// Take entire batch
						Some((Ok(batch), (input, remaining - batch_size)))
					} else {
						// Take partial batch - only first `remaining` rows
						let indices: Vec<usize> = (0..remaining).collect();
						let truncated = batch.extract_by_indices(&indices);
						Some((Ok(truncated), (input, 0)))
					}
				}
			}
		}))
	}
}
