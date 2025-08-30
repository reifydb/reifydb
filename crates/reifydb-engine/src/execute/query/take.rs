// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Transaction;

use crate::{
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct TakeNode<T: Transaction> {
	input: Box<ExecutionPlan<T>>,
	remaining: usize,
}

impl<T: Transaction> TakeNode<T> {
	pub(crate) fn new(input: Box<ExecutionPlan<T>>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
		}
	}
}

impl<T: Transaction> TakeNode<T> {
	pub(crate) fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut crate::StandardTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		while let Some(Batch {
			mut columns,
		}) = self.input.next(ctx, rx)?
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

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.input.layout()
	}
}
