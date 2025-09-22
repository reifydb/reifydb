// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{interface::Transaction, value::columnar::layout::ColumnsLayout};

use crate::execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode};

pub(crate) struct TakeNode<'a, T: Transaction> {
	input: Box<ExecutionPlan<'a, T>>,
	remaining: usize,
	initialized: Option<()>,
}

impl<'a, T: Transaction> TakeNode<'a, T> {
	pub(crate) fn new(input: Box<ExecutionPlan<'a, T>>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
			initialized: None,
		}
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for TakeNode<'a, T> {
	fn initialize(
		&mut self,
		rx: &mut crate::StandardTransaction<'a, T>,
		ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	fn next(&mut self, rx: &mut crate::StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.initialized.is_some(), "TakeNode::next() called before initialize()");

		while let Some(Batch {
			mut columns,
		}) = self.input.next(rx)?
		{
			let row_count = columns.row_count();
			if row_count == 0 {
				continue;
			}
			return if row_count <= self.remaining {
				self.remaining -= row_count;
				Ok(Some(Batch {
					columns,
				}))
			} else {
				columns.take(self.remaining)?;
				self.remaining = 0;
				Ok(Some(Batch {
					columns,
				}))
			};
		}
		Ok(None)
	}

	fn layout(&self) -> Option<ColumnsLayout<'a>> {
		self.input.layout()
	}
}
