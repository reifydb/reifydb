// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::{batch::head, column::headers::ColumnHeaders};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::reifydb_assertions;
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct TakeNode {
	input: Box<dyn QueryNode>,
	remaining: usize,
	initialized: Option<()>,
	emitted: bool,
}

impl TakeNode {
	pub(crate) fn new(input: Box<dyn QueryNode>, take: usize) -> Self {
		Self {
			input,
			remaining: take,
			initialized: None,
			emitted: false,
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
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.initialized.is_some(), "TakeNode::next() called before initialize()");
		}

		if self.remaining == 0 && self.emitted {
			return Ok(None);
		}

		let mut empty: Option<RecordBatch> = None;
		while let Some(columns) = self.input.next(rx, ctx)? {
			let transform_ctx = TransformContext {
				routines: &ctx.services.routines,
				runtime_context: &ctx.services.runtime_context,
				params: &ctx.params,
			};
			let result = self.apply(&transform_ctx, columns)?;
			if result.num_rows() == 0 && self.remaining > 0 {
				if empty.is_none() {
					empty = Some(result);
				}
				continue;
			}
			self.remaining -= result.num_rows();
			self.emitted = true;
			return Ok(Some(result));
		}
		if self.emitted {
			return Ok(None);
		}
		self.emitted = true;
		Ok(empty)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl Transform for TakeNode {
	fn apply(&self, _ctx: &TransformContext, input: RecordBatch) -> Result<RecordBatch> {
		if input.num_rows() > self.remaining {
			return Ok(head(&input, self.remaining));
		}
		Ok(input)
	}
}
