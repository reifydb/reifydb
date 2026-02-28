// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	Result,
	transform::{Transform, context::TransformContext},
	vm::volcano::query::{QueryContext, QueryNode},
};

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
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(name = "volcano::take::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "TakeNode::next() called before initialize()");

		if self.remaining == 0 {
			return Ok(None);
		}

		while let Some(columns) = self.input.next(rx, ctx)? {
			if columns.row_count() == 0 {
				continue;
			}
			let transform_ctx = TransformContext {
				functions: &ctx.services.functions,
				clock: &ctx.services.clock,
				params: &ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;
			self.remaining -= result.row_count();
			return Ok(Some(result));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl Transform for TakeNode {
	fn apply(&self, _ctx: &TransformContext, mut input: Columns) -> Result<Columns> {
		let row_count = input.row_count();
		if row_count > self.remaining {
			input.take(self.remaining)?;
		}
		Ok(input)
	}
}
