// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::engine::StandardEngine;
use reifydb_flow::transaction::substrate::FlowSubstrate;

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
		output_frontier::sweep(self.engine.single(), &self.substrate.frontiers);
	}
}
