// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::value::column::{Columns, headers::ColumnHeaders};

use crate::{
	StandardTransaction,
	environment::create_env_columns,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub(crate) struct EnvironmentNode {
	context: Option<Arc<ExecutionContext<'static>>>,
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

impl<'a> QueryNode<'a> for EnvironmentNode {
	fn initialize(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		// Store context with 'static lifetime for environment access
		self.context = Some(unsafe { std::mem::transmute(Arc::new(ctx.clone())) });
		Ok(())
	}

	fn next(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "EnvironmentNode::next() called before initialize()");

		// Environment executes once and returns environment dataframe
		if self.executed {
			return Ok(None);
		}

		let columns = create_env_columns();
		self.executed = true;

		// Transmute the columns to extend their lifetime
		// SAFETY: The columns are created here and genuinely have lifetime 'a
		let columns = unsafe { std::mem::transmute::<Columns<'_>, Columns<'a>>(columns) };

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		// Environment headers are known: "name" and "value" columns
		None
	}
}
