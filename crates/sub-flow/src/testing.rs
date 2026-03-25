// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::event::EventBus;
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{context::RuntimeContext, sync::mutex::Mutex};
use reifydb_transaction::{
	testing::TestingViewsChangeCaptor,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::Result;

use crate::{builder::OperatorFactory, engine::FlowEngine, transactional::interceptor::execute_inline_flow_changes};

pub(crate) struct ViewInlineTestingMutationCapture {
	pub engine: StandardEngine,
	pub catalog: Catalog,
	pub event_bus: EventBus,
	pub runtime_context: RuntimeContext,
	pub custom_operators: Arc<HashMap<String, OperatorFactory>>,
	cached_flow_engine: Mutex<Option<FlowEngine>>,
}

impl ViewInlineTestingMutationCapture {
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

		for flow_def in flows {
			let flow = load_flow_dag(&self.catalog, &mut Transaction::Admin(&mut *txn), flow_def.id)?;
			flow_engine.register_with_transaction(&mut Transaction::Admin(&mut *txn), flow)?;
		}

		Ok(flow_engine)
	}
}

impl TestingViewsChangeCaptor for ViewInlineTestingMutationCapture {
	fn capture(&self, txn: &mut AdminTransaction) -> Result<()> {
		let mut cached = self.cached_flow_engine.lock();
		if cached.is_none() {
			*cached = Some(self.build_flow_engine(txn)?);
		}
		let flow_engine = cached.as_ref().unwrap();
		txn.capture_testing_pre_commit(|ctx| {
			execute_inline_flow_changes(flow_engine, &self.engine, &self.catalog, ctx)
		})
	}
}
