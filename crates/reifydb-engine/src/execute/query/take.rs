// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{QueryTransaction, Transaction};

use crate::{
	StandardCommandTransaction,
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct TakeNode {
	input: Box<ExecutionPlan>,
	remaining: usize,
}

impl TakeNode {
	pub(crate) fn new(input: Box<ExecutionPlan>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
		}
	}
}

impl TakeNode {
	pub(crate) fn next<T: Transaction>(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut StandardCommandTransaction<T>,
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
