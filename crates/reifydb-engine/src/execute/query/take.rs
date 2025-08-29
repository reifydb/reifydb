// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::{
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct TakeNode<'a> {
	input: Box<ExecutionPlan<'a>>,
	remaining: usize,
}

impl<'a> TakeNode<'a> {
	pub(crate) fn new(input: Box<ExecutionPlan<'a>>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
		}
	}
}

impl<'a> TakeNode<'a> {
	pub(crate) fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut impl QueryTransaction,
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
