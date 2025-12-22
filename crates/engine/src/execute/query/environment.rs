// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::value::column::{Columns, headers::ColumnHeaders};

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

#[async_trait]
impl QueryNode for EnvironmentNode {
	async fn initialize<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		// Store context for environment access
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	async fn next<'a>(
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
