// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_engine::engine::StandardEngine;
use reifydb_flow_async::transaction::substrate::FlowSubstrate;
use reifydb_runtime::context::transaction::TransactionContext;

use crate::progress::output_frontier;

pub struct FlowShutdownState {
	engine: StandardEngine,
	substrate: FlowSubstrate,
}

impl FlowShutdownState {
	pub fn new(engine: StandardEngine, substrate: FlowSubstrate) -> Self {
		Self {
			engine,
			substrate,
		}
	}

	pub fn persist_frontiers(&self) {
		if let Err(e) = output_frontier::try_sweep(
			&Arc::new(TransactionContext::system(self.engine.runtime_context().clone())),
			self.engine.single(),
			&self.substrate.frontiers,
		) {
			panic!("flow shutdown could not persist its output frontiers: {e}");
		}
	}
}
