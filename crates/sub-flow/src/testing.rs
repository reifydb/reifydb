// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{event::EventBus, interface::catalog::primitive::PrimitiveId};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{context::RuntimeContext, sync::mutex::Mutex};
use reifydb_transaction::{
	testing::TestFlowProcessor,
	transaction::{TestTransaction, Transaction, admin::AdminTransaction},
};
use reifydb_type::Result;

use crate::{builder::OperatorFactory, engine::FlowEngine, transactional::interceptor::execute_inline_flow_changes};

pub(crate) struct StandardTestFlowProcessor {
	pub engine: StandardEngine,
	pub catalog: Catalog,
	pub event_bus: EventBus,
	pub runtime_context: RuntimeContext,
	pub custom_operators: Arc<HashMap<String, OperatorFactory>>,
	cached_flow_engine: Mutex<Option<FlowEngine>>,
}

impl StandardTestFlowProcessor {
	pub fn new(
		engine: StandardEngine,
		catalog: Catalog,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		custom_operators: Arc<HashMap<String, OperatorFactory>>,
	) -> Self {
		Self {
			engine,
			catalog,
			event_bus,
			runtime_context,
			custom_operators,
			cached_flow_engine: Mutex::new(None),
		}
	}

	fn build_flow_engine(&self, txn: &mut AdminTransaction) -> Result<FlowEngine> {
		let mut flow_engine = FlowEngine::new(
			self.catalog.clone(),
			self.engine.executor(),
			self.event_bus.clone(),
			self.runtime_context.clone(),
			self.custom_operators.clone(),
		);

		let flows = self.catalog.list_flows_all(&mut Transaction::Admin(&mut *txn))?;

		for flow in flows {
			let flow = load_flow_dag(&self.catalog, &mut Transaction::Admin(&mut *txn), flow.id)?;
			flow_engine.register_with_transaction(&mut Transaction::Admin(&mut *txn), flow)?;
		}

		Ok(flow_engine)
	}
}

impl TestFlowProcessor for StandardTestFlowProcessor {
	fn process(&self, txn: &mut TestTransaction<'_>) -> Result<()> {
		let has_source_changes = txn
			.inner
			.accumulator_entries_from(txn.baseline)
			.iter()
			.any(|(id, _)| !matches!(id, PrimitiveId::View(_)));
		if !has_source_changes {
			return Ok(());
		}

		let mut cached = self.cached_flow_engine.lock();
		if cached.is_none() {
			*cached = Some(self.build_flow_engine(txn.inner)?);
		}
		let flow_engine = cached.as_ref().unwrap();
		txn.inner.capture_testing_pre_commit_from(txn.baseline, |ctx| {
			execute_inline_flow_changes(flow_engine, &self.engine, &self.catalog, ctx)
		})
	}
}
