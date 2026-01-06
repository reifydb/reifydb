// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::headers::ColumnHeaders;

use crate::{
	StandardTransaction,
	environment::create_env_columns,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub(crate) struct EnvironmentNode {
	context: Option<Arc<ExecutionContext>>,
	executed: bool,
}

impl EnvironmentNode {
	pub fn new() -> Self {
		Self {
			context: None,
			executed: false,
		}
	}
}

impl QueryNode for EnvironmentNode {
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		// Store context for environment access
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "EnvironmentNode::next() called before initialize()");

		// Environment executes once and returns environment dataframe
		if self.executed {
			return Ok(None);
		}

		let columns = create_env_columns();
		self.executed = true;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		// Environment headers are known: "name" and "value" columns
		None
	}
}
