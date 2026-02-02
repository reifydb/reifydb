// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	internal,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;

use crate::vm::volcano::query::{QueryContext, QueryNode, QueryPlan};

pub(crate) struct ScalarizeNode {
	input: Box<QueryPlan>,
	initialized: Option<()>,
	frame_consumed: bool,
}

impl<'a> ScalarizeNode {
	pub(crate) fn new(input: Box<QueryPlan>) -> Self {
		Self {
			input,
			initialized: None,
			frame_consumed: false,
		}
	}
}

impl QueryNode for ScalarizeNode {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		self.frame_consumed = false;
		Ok(())
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "ScalarizeNode::next() called before initialize()");

		// Scalarize nodes should only produce one result
		if self.frame_consumed {
			return Ok(None);
		}

		// Get the input frame
		let input_batch = match self.input.next(rx, ctx)? {
			Some(batch) => batch,
			None => {
				// Empty input - return empty result
				self.frame_consumed = true;
				return Ok(None);
			}
		};

		// Check frame dimensions
		let column_count = input_batch.len();
		let row_count = if column_count == 0 {
			0
		} else {
			input_batch[0].data.len()
		};

		match (row_count, column_count) {
			(1, 1) => {
				// Valid 1x1 frame - return as-is
				self.frame_consumed = true;
				Ok(Some(input_batch))
			}
			(0, _) => {
				// Empty frame - return empty result
				self.frame_consumed = true;
				Ok(None)
			}
			(rows, cols) => {
				// Error for non-1x1 frames
				Err(reifydb_type::error::Error(internal!(
					"Cannot scalarize frame with {} rows and {} columns - expected 1x1 frame",
					rows,
					cols
				)))
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		// Headers are passed through from input
		self.input.headers()
	}
}
