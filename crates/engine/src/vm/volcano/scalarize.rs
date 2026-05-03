// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	internal,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct ScalarizeNode {
	input: Box<dyn QueryNode>,
	initialized: Option<()>,
	frame_consumed: bool,
}

impl ScalarizeNode {
	pub(crate) fn new(input: Box<dyn QueryNode>) -> Self {
		Self {
			input,
			initialized: None,
			frame_consumed: false,
		}
	}
}

impl QueryNode for ScalarizeNode {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		self.frame_consumed = false;
		Ok(())
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "ScalarizeNode::next() called before initialize()");

		if self.frame_consumed {
			return Ok(None);
		}

		let input_batch = match self.input.next(rx, ctx)? {
			Some(batch) => batch,
			None => {
				self.frame_consumed = true;
				return Ok(None);
			}
		};

		let column_count = input_batch.len();
		let row_count = if column_count == 0 {
			0
		} else {
			input_batch[0].len()
		};

		match (row_count, column_count) {
			(1, 1) => {
				self.frame_consumed = true;
				Ok(Some(input_batch))
			}
			(0, _) => {
				self.frame_consumed = true;
				Ok(None)
			}
			(rows, cols) => Err(Error(Box::new(internal!(
				"Cannot scalarize frame with {} rows and {} columns - expected 1x1 frame",
				rows,
				cols
			)))),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
