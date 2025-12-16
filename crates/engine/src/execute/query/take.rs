// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::headers::ColumnHeaders;
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode};

pub(crate) struct TakeNode<'a> {
	input: Box<ExecutionPlan<'a>>,
	remaining: usize,
	initialized: Option<()>,
}

impl<'a> TakeNode<'a> {
	pub(crate) fn new(input: Box<ExecutionPlan<'a>>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
			initialized: None,
		}
	}
}

impl<'a> QueryNode<'a> for TakeNode<'a> {
	#[instrument(name = "TakeNode::initialize", level = "trace", skip_all)]
	fn initialize(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(name = "TakeNode::next", level = "trace", skip_all)]
	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.initialized.is_some(), "TakeNode::next() called before initialize()");

		if self.remaining == 0 {
			return Ok(None);
		}

		while let Some(Batch {
			mut columns,
		}) = self.input.next(rx, ctx)?
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

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		self.input.headers()
	}
}
