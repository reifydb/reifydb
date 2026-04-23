// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	collections::HashMap,
	sync::{Arc, RwLock},
	time::Duration,
};

use dashmap::DashMap;
use reifydb_cdc::{
	consume::{
		consumer::CdcConsumer,
		poll::{PollConsumer, PollConsumerConfig},
	},
	storage::CdcStore,
};
use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	error::diagnostic::catalog::subscription_not_found,
	interface::{
		WithEventBus,
		catalog::{
			flow::FlowId,
			id::SubscriptionId,
			subscription::{SubscriptionInspector, SubscriptionInspectorRef},
		},
		cdc::CdcConsumerId,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{SubscriptionService, SubscriptionServiceRef},
};
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_runtime::{SharedRuntime, context::RuntimeContext};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_sub_flow::{builder::OperatorFactory, engine::FlowEngine, operator::Operators};
use reifydb_transaction::{interceptor::builder::InterceptorBuilder, transaction::Transaction};
use reifydb_type::{Result, error::Error, fragment::Fragment, value::row_number::RowNumber};

use crate::{
	consumer::SubscriptionCdcConsumer,
	sink::{DeliveryBuffer, EphemeralSinkSubscriptionOperator},
	store::SubscriptionStore,
};

/// Internal shared state for the subscription subsystem.
///
/// Wrapped in Arc so both the subsystem and the service handle can access it.
struct SubscriptionState {
	store: Arc<SubscriptionStore>,
	flow_engine: Arc<RwLock<FlowEngine>>,
	flow_states: Arc<DashMap<FlowId, HashMap<EncodedKey, EncodedRow>>>,
	/// Mapping from subscription_id to flow_id for lifecycle management.
	subscription_flows: RwLock<HashMap<SubscriptionId, FlowId>>,
	/// Staged delivery buffer - sinks push here, CDC consumer commits per batch.
	delivery: Arc<DeliveryBuffer>,
}

/// Service handle implementing the engine's SubscriptionService trait.
///
/// This is registered in IoC and used by DDL and transport layers.
struct SubscriptionServiceImpl {
	state: Arc<SubscriptionState>,
}

impl SubscriptionService for SubscriptionServiceImpl {
	fn register_subscription(
		&self,
		id: SubscriptionId,
		flow_dag: FlowDag,
		column_names: Vec<String>,
		txn: &mut Transaction<'_>,
	) -> Result<()> {
		// 1. Register buffer in store with column schema
		self.state.store.register(id, column_names);

		// 2. Register flow in engine, replacing SinkSubscription with ephemeral operator
		let flow_id = flow_dag.id;
		{
			let mut engine = self.state.flow_engine.write().unwrap();
			register_ephemeral_flow(&mut engine, txn, flow_dag, id, self.state.delivery.clone())?;
		}

		// 3. Track mapping
		self.state.subscription_flows.write().unwrap().insert(id, flow_id);

		// 4. Initialize empty flow state
		self.state.flow_states.insert(flow_id, HashMap::new());

		Ok(())
	}

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<()> {
		let existed = self.state.store.unregister(id);

		if let Some(flow_id) = self.state.subscription_flows.write().unwrap().remove(id) {
			// Remove flow state
			self.state.flow_states.remove(&flow_id);
			// Clean up the flow from the engine
			self.state.flow_engine.write().unwrap().remove_flow(flow_id);
		}

		if existed {
			Ok(())
		} else {
			Err(Error(Box::new(subscription_not_found(
				Fragment::internal(format!("subscription_{}", id.0)),
				&format!("subscription_{}", id.0),
			))))
		}
	}

	fn next_id(&self) -> SubscriptionId {
		self.state.store.next_id()
	}
}

/// Register a subscription flow in the engine, replacing SinkSubscription nodes
/// with EphemeralSinkSubscriptionOperator wrapped as Custom.
fn register_ephemeral_flow(
	engine: &mut FlowEngine,
	txn: &mut Transaction<'_>,
	flow: FlowDag,
	subscription_id: SubscriptionId,
	delivery: Arc<DeliveryBuffer>,
) -> Result<()> {
	for node_id in flow.topological_order()? {
		let node = flow.get_node(&node_id).unwrap();
		match &node.ty {
			FlowNodeType::SinkSubscription {
				..
			} => {
				// Replace with ephemeral operator
				let parent = engine
					.operators
					.get(&node.inputs[0])
					.expect("Parent operator not found")
					.clone();
				let op = EphemeralSinkSubscriptionOperator::new(
					parent,
					node_id,
					subscription_id,
					delivery.clone(),
				);
				engine.operators.insert(node_id, Arc::new(Operators::Custom(Box::new(op))));
			}
			_ => {
				engine.add(txn, &flow, node)?;
			}
		}
	}
	engine.analyzer.add(flow.clone());
	engine.flows.insert(flow.id, flow);
	Ok(())
}

/// Ephemeral subscription subsystem.
///
/// Owns an independent CDC consumer that processes subscription flows in-memory.
pub struct SubscriptionSubsystem {
	consumer: PollConsumer<StandardEngine, SubscriptionCdcConsumer>,
	state: Arc<SubscriptionState>,
	running: bool,
}

impl SubscriptionSubsystem {
	pub fn new(
		engine: StandardEngine,
		cdc_store: CdcStore,
		store: Arc<SubscriptionStore>,
		runtime_context: RuntimeContext,
		custom_operators: Arc<HashMap<String, OperatorFactory>>,
	) -> Self {
		let catalog = engine.catalog();
		let executor = engine.executor();
		let event_bus = engine.event_bus().clone();
		let multi = engine.multi_owned();
		let actor_system = engine.actor_system();

		let flow_engine = Arc::new(RwLock::new(FlowEngine::new(
			catalog.clone(),
			executor,
			event_bus,
			runtime_context,
			custom_operators,
		)));

		let flow_states = Arc::new(DashMap::new());
		let delivery = Arc::new(DeliveryBuffer::new(store.clone()));

		let state = Arc::new(SubscriptionState {
			store,
			flow_engine: flow_engine.clone(),
			flow_states: flow_states.clone(),
			subscription_flows: RwLock::new(HashMap::new()),
			delivery: delivery.clone(),
		});

		let cdc_consumer = SubscriptionCdcConsumer::new(flow_engine, multi, catalog, flow_states, delivery);

		let config = PollConsumerConfig::new(
			CdcConsumerId::new("__SUBSCRIPTION_CONSUMER"),
			"sub-subscription-poll",
			Duration::from_millis(10),
			None,
		);

		let consumer = PollConsumer::new(config, engine, cdc_consumer, cdc_store, actor_system);

		Self {
			consumer,
			state,
			running: false,
		}
	}

	/// Get a service handle for IoC registration.
	pub fn service_handle(&self) -> SubscriptionServiceRef {
		Arc::new(SubscriptionServiceImpl {
			state: self.state.clone(),
		})
	}

	/// Get the subscription store.
	pub fn store(&self) -> &Arc<SubscriptionStore> {
		&self.state.store
	}
}

impl Subsystem for SubscriptionSubsystem {
	fn name(&self) -> &'static str {
		"sub-subscription"
	}

	fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(());
		}
		self.consumer.start()?;
		self.running = true;
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}
		self.consumer.stop()?;
		self.running = false;
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running
	}

	fn health_status(&self) -> HealthStatus {
		if self.running {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

impl HasVersion for SubscriptionSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Ephemeral subscription subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

/// Implementation of SubscriptionInspector for IoC registration.
struct SubscriptionInspectorImpl {
	store: Arc<SubscriptionStore>,
}

impl SubscriptionInspector for SubscriptionInspectorImpl {
	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.store.active_subscriptions()
	}

	fn column_count(&self, id: &SubscriptionId) -> Option<usize> {
		self.store.column_names(id).map(|v| v.len())
	}

	fn inspect(&self, id: SubscriptionId) -> Option<Columns> {
		let batches = self.store.drain(&id, usize::MAX);
		if batches.is_empty() {
			// Return schema-only empty Columns if subscription exists
			let names = self.store.column_names(&id)?;
			let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
			return Some(Columns::from_rows(&name_refs, &[]));
		}
		if batches.len() == 1 {
			return Some(batches.into_iter().next().unwrap());
		}

		// Merge multiple batches: collect all column names from first batch,
		// then collect all rows across batches.
		let first = &batches[0];
		let names: Vec<&str> = first.iter().map(|c| c.name.text()).collect();

		let mut all_rows = Vec::new();
		let mut all_row_numbers = Vec::new();

		for batch in &batches {
			for i in 0..batch.row_count() {
				all_rows.push(batch.get_row(i));
				if i < batch.row_numbers.len() {
					all_row_numbers.push(batch.row_numbers[i]);
				} else {
					all_row_numbers.push(RowNumber(0));
				}
			}
		}

		Some(Columns::from_rows_with_row_numbers(&names, &all_rows, all_row_numbers))
	}
}

/// Factory for creating the subscription subsystem.
pub struct SubscriptionSubsystemFactory;

impl SubsystemFactory for SubscriptionSubsystemFactory {
	fn provide_interceptors(&self, builder: InterceptorBuilder, _ioc: &IocContainer) -> InterceptorBuilder {
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let cdc_store = ioc.resolve::<CdcStore>()?;
		let runtime = ioc.resolve::<SharedRuntime>()?;

		let runtime_context = RuntimeContext::with_clock(runtime.clock().clone());
		let store = Arc::new(SubscriptionStore::new(1024));
		let custom_operators = Arc::new(HashMap::new());

		let subsystem =
			SubscriptionSubsystem::new(engine, cdc_store, store.clone(), runtime_context, custom_operators);

		// Register services in IoC (not the subsystem itself)
		let service = subsystem.service_handle();
		ioc.register_service::<SubscriptionServiceRef>(service);
		ioc.register_service::<Arc<SubscriptionStore>>(store.clone());

		let inspector: SubscriptionInspectorRef = Arc::new(SubscriptionInspectorImpl {
			store,
		});
		ioc.register_service::<SubscriptionInspectorRef>(inspector);

		Ok(Box::new(subsystem))
	}
}
