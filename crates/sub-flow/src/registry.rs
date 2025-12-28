// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Flow registry for tracking active flows and their tasks.

use std::{
	collections::{HashMap, HashSet},
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant},
};

use reifydb_core::{
	CommitVersion, Error, Result,
	interface::{PrimitiveId, WithEventBus, catalog::FlowId},
};
use reifydb_engine::{StandardColumnEvaluator, StandardEngine};
use reifydb_rql::flow::Flow;
use reifydb_sdk::FlowChange;
use reifydb_type::diagnostic::flow::{flow_already_registered, flow_backfill_timeout};
use tokio::{
	sync::{RwLock, mpsc},
	task::JoinHandle,
	time::sleep,
};

use crate::{
	FlowEngine, builder::OperatorFactory, coordinator::coordinate_task,
	operator::transform::registry::TransformOperatorRegistry,
};

/// Handle to an active flow's task and communication channels.
pub struct FlowHandle {
	/// Sender for dispatching batches to this flow.
	pub tx: mpsc::UnboundedSender<Vec<FlowChange>>,

	/// Task handle for the flow coordinator.
	pub task: JoinHandle<()>,

	/// Sources this flow subscribes to.
	pub sources: HashSet<PrimitiveId>,

	/// Current processed version (shared with coordinator task).
	pub version: Arc<AtomicU64>,
}

/// Registry of active flows and routing information.
pub struct FlowRegistry {
	/// Map of flow ID to flow handle.
	pub(crate) flows: RwLock<HashMap<FlowId, FlowHandle>>,

	/// Reverse index: source ID → flow IDs that subscribe to it.
	pub(crate) source_to_flows: RwLock<HashMap<PrimitiveId, Vec<FlowId>>>,

	/// Engine for database operations.
	engine: StandardEngine,

	/// Shared FlowEngine for all tasks.
	flow_engine: FlowEngine,
}

impl FlowRegistry {
	/// Create a new empty registry.
	pub fn new(
		engine: StandardEngine,
		operators: Vec<(String, OperatorFactory)>,
		operators_dir: Option<PathBuf>,
	) -> Self {
		// Create shared FlowEngine once
		let flow_engine = create_flow_engine(&engine, &operators, operators_dir.as_ref());

		Self {
			flows: RwLock::new(HashMap::new()),
			source_to_flows: RwLock::new(HashMap::new()),
			engine,
			flow_engine,
		}
	}

	/// Register a new flow with backfill.
	///
	/// Called when a new flow is created. The flow will backfill from source
	/// tables at the given version before processing incremental changes.
	///
	/// This function blocks until backfill completes, ensuring synchronous semantics.
	pub async fn register_with_backfill(
		&self,
		flow: Flow,
		sources: HashSet<PrimitiveId>,
		backfill_version: CommitVersion,
	) -> Result<()> {
		let flow_id = flow.id;

		// Guard against duplicate registration
		{
			let flows = self.flows.read().await;
			if flows.contains_key(&flow_id) {
				return Err(Error(flow_already_registered(flow_id.0)));
			}
		}

		let (tx, rx) = mpsc::unbounded_channel();
		let version = Arc::new(AtomicU64::new(0));

		// Spawn the flow coordinator task
		let task = tokio::spawn(coordinate_task(
			flow_id,
			rx,
			flow,
			self.engine.clone(),
			version.clone(),
			Some(backfill_version),
			self.flow_engine.clone(),
		));

		let handle = FlowHandle {
			tx,
			task,
			sources: sources.clone(),
			version: version.clone(),
		};

		// Update flows map
		{
			let mut flows = self.flows.write().await;
			flows.insert(flow_id, handle);
		}

		// Update source→flow mapping
		{
			let mut source_map = self.source_to_flows.write().await;
			for source in sources {
				source_map.entry(source).or_default().push(flow_id);
			}
		}

		// Wait for backfill to complete before returning.
		// The task sets version to backfill_version when done.
		let timeout = Duration::from_secs(300); // 5 minute timeout
		let start = Instant::now();
		let poll_interval = Duration::from_millis(10);

		loop {
			let current = version.load(Ordering::Acquire);
			if current >= backfill_version.0 {
				break;
			}

			if start.elapsed() >= timeout {
				return Err(Error(flow_backfill_timeout(flow_id.0, 300)));
			}

			sleep(poll_interval).await;
		}

		tracing::info!(
			flow_id = flow_id.0,
			backfill_version = backfill_version.0,
			"registered flow with backfill"
		);

		Ok(())
	}

	/// Register an existing flow (on startup).
	///
	/// Called for flows that already exist in catalog with persisted versions.
	/// Does NOT trigger backfill - flow resumes from persisted_version.
	pub async fn register(
		&self,
		flow: Flow,
		sources: HashSet<PrimitiveId>,
		persisted_version: CommitVersion,
	) -> Result<()> {
		let flow_id = flow.id;

		// Guard against duplicate registration
		{
			let flows = self.flows.read().await;
			if flows.contains_key(&flow_id) {
				return Err(Error(flow_already_registered(flow_id.0)));
			}
		}

		let (tx, rx) = mpsc::unbounded_channel();
		// Initialize version from persisted value (not 0)
		let version = Arc::new(AtomicU64::new(persisted_version.0));

		// Spawn the flow coordinator task WITHOUT backfill
		let task = tokio::spawn(coordinate_task(
			flow_id,
			rx,
			flow,
			self.engine.clone(),
			version.clone(),
			None, // No backfill - flow already has data
			self.flow_engine.clone(),
		));

		let handle = FlowHandle {
			tx,
			task,
			sources: sources.clone(),
			version: version.clone(),
		};

		// Update flows map
		{
			let mut flows = self.flows.write().await;
			flows.insert(flow_id, handle);
		}

		// Update source→flow mapping
		{
			let mut source_map = self.source_to_flows.write().await;
			for source in sources {
				source_map.entry(source).or_default().push(flow_id);
			}
		}

		tracing::debug!(flow_id = flow_id.0, version = persisted_version.0, "registered existing flow");

		Ok(())
	}

	/// Deregister a flow and return its task handle.
	///
	/// Removes the flow from the registry. The channel sender is dropped,
	/// which will cause the task to exit after processing remaining batches.
	pub async fn deregister(&self, flow_id: FlowId) -> Option<JoinHandle<()>> {
		// Remove from flows map
		let handle = {
			let mut flows = self.flows.write().await;
			flows.remove(&flow_id)
		};

		let handle = handle?;

		// Remove from source→flow mapping
		{
			let mut source_map = self.source_to_flows.write().await;
			for source in &handle.sources {
				if let Some(flow_ids) = source_map.get_mut(source) {
					flow_ids.retain(|id| *id != flow_id);
					if flow_ids.is_empty() {
						source_map.remove(source);
					}
				}
			}
		}

		tracing::info!(flow_id = flow_id.0, "deregistered flow");

		Some(handle.task)
	}

	/// Check if a flow is registered.
	pub async fn contains(&self, flow_id: FlowId) -> bool {
		let flows = self.flows.read().await;
		flows.contains_key(&flow_id)
	}

	/// Get all registered flow IDs.
	pub async fn flow_ids(&self) -> Vec<FlowId> {
		let flows = self.flows.read().await;
		flows.keys().copied().collect()
	}

	/// Get all flow data for lag computation.
	///
	/// Returns tuples of (flow_id, current_version, sources).
	pub async fn all_flow_data(&self) -> Vec<(FlowId, u64, HashSet<PrimitiveId>)> {
		let flows = self.flows.read().await;
		flows.iter()
			.map(|(&flow_id, handle)| {
				(flow_id, handle.version.load(Ordering::Acquire), handle.sources.clone())
			})
			.collect()
	}
}

/// Create a FlowEngine instance.
fn create_flow_engine(
	engine: &StandardEngine,
	operators: &[(String, OperatorFactory)],
	operators_dir: Option<&PathBuf>,
) -> FlowEngine {
	let mut registry = TransformOperatorRegistry::new();

	// Register custom operator factories
	for (name, factory) in operators.iter() {
		let factory = factory.clone();
		let name = name.clone();
		registry.register(name, move |node, exprs| factory(node, exprs));
	}

	FlowEngine::new(
		StandardColumnEvaluator::default(),
		engine.executor(),
		registry,
		engine.event_bus().clone(),
		operators_dir.cloned(),
	)
}
