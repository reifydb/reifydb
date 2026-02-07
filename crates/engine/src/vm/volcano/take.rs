// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::vm::volcano::query::{QueryContext, QueryNode};

pub(crate) struct TakeNode {
	input: Box<dyn QueryNode>,
	remaining: usize,
	initialized: Option<()>,
}

impl TakeNode {
	pub(crate) fn new(input: Box<dyn QueryNode>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
			initialized: None,
		}
	}
}

impl QueryNode for TakeNode {
	#[instrument(name = "volcano::take::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(name = "volcano::take::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "TakeNode::next() called before initialize()");

		if self.remaining == 0 {
			return Ok(None);
		}

		while let Some(mut columns) = self.input.next(rx, ctx)? {
			let row_count = columns.row_count();
			if row_count == 0 {
				continue;
			}
			return if row_count <= self.remaining {
				self.remaining -= row_count;
				Ok(Some(columns))
			} else {
				columns.take(self.remaining)?;
				self.remaining = 0;
				Ok(Some(columns))
			};
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
