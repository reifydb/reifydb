// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::headers::ColumnHeaders;
use reifydb_type::{Fragment, internal_error};

use crate::execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode};

pub(crate) struct ScalarizeNode<'a> {
	input: Box<ExecutionPlan<'a>>,
	initialized: Option<()>,
	frame_consumed: bool,
}

impl<'a> ScalarizeNode<'a> {
	pub(crate) fn new(input: Box<ExecutionPlan<'a>>) -> Self {
		Self {
			input,
			initialized: None,
			frame_consumed: false,
		}
	}
}

impl<'a> QueryNode<'a> for ScalarizeNode<'a> {
	fn initialize(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		self.frame_consumed = false;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
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
		let column_count = input_batch.columns.len();
		let row_count = if column_count == 0 {
			0
		} else {
			input_batch.columns[0].data.len()
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
				Err(reifydb_type::Error(internal_error!(
					"Cannot scalarize frame with {} rows and {} columns - expected 1x1 frame",
					rows,
					cols
				)))
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		// Headers are passed through from input
		self.input.headers()
	}
}
