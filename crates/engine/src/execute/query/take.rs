// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::headers::ColumnHeaders;
use reifydb_transaction::standard::StandardTransaction;
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode};

pub(crate) struct TakeNode {
	input: Box<ExecutionPlan>,
	remaining: usize,
	initialized: Option<()>,
}

impl TakeNode {
	pub(crate) fn new(input: Box<ExecutionPlan>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
			initialized: None,
		}
	}
}

impl QueryNode for TakeNode {
	#[instrument(name = "query::take::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(name = "query::take::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
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

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
