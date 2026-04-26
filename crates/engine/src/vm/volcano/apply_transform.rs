// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct ApplyTransformNode {
	input: Box<dyn QueryNode>,
	transform: Box<dyn Transform>,
	context: Option<Arc<QueryContext>>,
}

impl ApplyTransformNode {
	pub fn new(input: Box<dyn QueryNode>, transform: Box<dyn Transform>) -> Self {
		Self {
			input,
			transform,
			context: None,
		}
	}
}

impl QueryNode for ApplyTransformNode {
	#[instrument(level = "trace", skip_all, name = "volcano::apply_transform::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::apply_transform::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "ApplyTransformNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if let Some(columns) = self.input.next(rx, ctx)? {
			let transform_ctx = TransformContext {			routines: &ctx.services.routines,
				runtime_context: &stored_ctx.services.runtime_context,
				params: &stored_ctx.params,
			};
			let result = self.transform.apply(&transform_ctx, columns)?;
			Ok(Some(result))
		} else {
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
