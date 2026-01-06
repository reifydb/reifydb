// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{error::Result, pipeline::Pipeline};

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
		Box::new(TakeIterator {
			input,
			remaining: self.limit,
		})
	}
}

/// Iterator that limits the number of rows returned
struct TakeIterator {
	input: Pipeline,
	remaining: usize,
}

impl Iterator for TakeIterator {
	type Item = Result<reifydb_core::Batch>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.remaining == 0 {
			return None;
		}

		let batch = match self.input.next()? {
			Ok(b) => b,
			Err(e) => {
				self.remaining = 0;
				return Some(Err(e));
			}
		};

		let batch_size = batch.row_count();

		if batch_size <= self.remaining {
			// Take entire batch
			self.remaining -= batch_size;
			Some(Ok(batch))
		} else {
			// Take partial batch - only first `remaining` rows
			let indices: Vec<usize> = (0..self.remaining).collect();
			let truncated = batch.extract_by_indices(&indices);
			self.remaining = 0;
			Some(Ok(truncated))
		}
	}
}
