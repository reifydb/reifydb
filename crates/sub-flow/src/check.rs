// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::flow::dag::FlowDag;
use reifydb_engine::{engine::StandardEngine, flow_check::FlowRegistrationCheck};
use reifydb_flow_async::{
	engine::FlowEngineInner, operator::metrics::OperatorSampleRegistry, transaction::substrate::FlowSubstrate,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::Result;

use crate::{builder::CustomOperators, operator::provider::StandardOperatorProvider};

pub(crate) struct FlowRegistrationProbe {
	pub(crate) engine: StandardEngine,
	pub(crate) custom_operators: CustomOperators,
	pub(crate) substrate: FlowSubstrate,
}

impl FlowRegistrationCheck for FlowRegistrationProbe {
	fn check(&self, txn: &mut AdminTransaction, dag: &FlowDag) -> Result<()> {
		let mut probe = FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor().routines.clone(),
			RuntimeContext::builder(self.engine.runtime_context().spawner.clone())
				.clock(self.engine.runtime_context().clock.clone())
				.rng(self.engine.runtime_context().rng.fork(b"sub-flow/check"))
				.build(),
			Arc::new(StandardOperatorProvider::new(self.custom_operators.clone())),
			self.substrate.clone(),
			OperatorSampleRegistry::new(),
		);
		probe.register_with_transaction(&mut Transaction::Admin(txn), dag.clone())
	}
}
