// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Single-threaded flow loop using CDC PollConsumer.

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
	sync::Arc,
	time::Duration,
};

use parking_lot::RwLock;
use reifydb_cdc::{CdcCheckpoint, CdcConsume, CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcChange, CdcConsumerId, FlowId, PrimitiveId},
	key::{Key, KeyKind},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_rql::flow::{Flow, load_flow};
use reifydb_sdk::FlowChange;
use reifydb_transaction::cdc::CdcQueryTransaction;
use tracing::{debug, error, info, warn};

use crate::{
	FlowEngine, FlowTransaction, catalog::FlowCatalog, consumer::FlowConsumer, convert::convert_cdc_to_flow_change,
	tracker::PrimitiveVersionTracker,
};

/// Configuration for the flow loop.
pub struct FlowLoopConfig {
	/// Poll interval for checking new CDC events.
	pub poll_interval: Duration,
	/// Consumer ID for checkpoint persistence.
	pub consumer_id: CdcConsumerId,
}

impl Default for FlowLoopConfig {
	fn default() -> Self {
		Self {
			poll_interval: Duration::from_millis(1),
			consumer_id: CdcConsumerId::new("flow-loop"),
		}
	}
}

/// Decoded CDC changes for a single version.
pub struct DecodedChanges {
	/// The decoded flow changes.
	pub changes: Vec<FlowChange>,
	/// Set of primitives affected by these changes.
	pub primitives: HashSet<PrimitiveId>,
}

/// Main flow loop that uses CDC PollConsumer.
pub struct FlowLoop {
	consumer: Option<PollConsumer<FlowLoopConsumer>>,
}

impl FlowLoop {
	/// Create a new flow loop.
	pub fn new(
		engine: StandardEngine,
		flow_engine: Arc<FlowEngine>,
		primitive_tracker: Arc<PrimitiveVersionTracker>,
		config: FlowLoopConfig,
	) -> Self {
		let catalog_cache = FlowCatalog::new(engine.catalog());

		let consume_impl = FlowLoopConsumer {
			engine: engine.clone(),
			flow_engine,
			primitive_tracker,
			catalog_cache,
			consumers: RwLock::new(HashMap::new()),
		};

		let poll_config = PollConsumerConfig::new(config.consumer_id, config.poll_interval, None);

		let consumer = PollConsumer::new(poll_config, engine, consume_impl);

		Self {
			consumer: Some(consumer),
		}
	}

	/// Start the flow loop.
	pub fn start(&mut self) -> Result<()> {
		debug!("starting flow loop");

		if let Some(ref mut consumer) = self.consumer {
			consumer.start()?;
		}

		info!("flow loop started");
		Ok(())
	}

	/// Stop the flow loop gracefully.
	pub fn stop(&mut self) -> Result<()> {
		debug!("stopping flow loop");

		if let Some(ref mut consumer) = self.consumer {
			consumer.stop()?;
		}

		info!("flow loop stopped");
		Ok(())
	}
}

/// Implementation of CDC consume logic for the flow loop.
struct FlowLoopConsumer {
	engine: StandardEngine,
	flow_engine: Arc<FlowEngine>,
	primitive_tracker: Arc<PrimitiveVersionTracker>,
	catalog_cache: FlowCatalog,
	consumers: RwLock<HashMap<FlowId, FlowConsumer>>,
}

impl CdcConsume for FlowLoopConsumer {
	fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		let catalog = self.engine.catalog();

		// Use a single FlowTransaction for all CDCs in this batch.
		// This ensures state from version N is visible to version N+1
		// because reads check self.pending first (which accumulates all writes).
		let mut flow_txn: Option<FlowTransaction> = None;

		for cdc in cdcs {
			let version = cdc.version;

			// Step 1: Update primitive tracker
			for change in &cdc.changes {
				if let Some(Key::Row(row_key)) = Key::decode(change.key()) {
					self.primitive_tracker.update(row_key.primitive, version);
				}
			}

			// Step 2: Check for new flow registrations
			let new_flows = self.detect_new_flows(txn, &cdc)?;

			// Step 3: Backfill new flows (blocking)
			for (flow_id, flow) in new_flows {
				self.backfill_flow(txn, flow_id, &flow, version)?;

				// Create consumer for the new flow
				let consumer = FlowConsumer::new(flow_id, flow);
				consumer.set_version(version);
				self.consumers.write().insert(flow_id, consumer);
			}

			// Step 4: Decode CDC once for all flows
			let decoded = self.decode_cdc(&cdc, version)?;

			// Step 5: Create or update the FlowTransaction
			match &mut flow_txn {
				None => {
					flow_txn = Some(FlowTransaction::new(txn, version, catalog.clone()));
				}
				Some(ft) => {
					ft.update_version(version);
				}
			}

			// Step 6: Process version for all active flows
			{
				let consumers = self.consumers.read();
				for (flow_id, consumer) in consumers.iter() {
					// Fast path: check if any sources were affected
					if !consumer.has_sources(&decoded.primitives) {
						continue;
					}

					if let Err(e) = consumer.process(
						flow_txn.as_mut().unwrap(),
						&self.flow_engine,
						&decoded.changes,
					) {
						error!(flow_id = flow_id.0, error = %e, "failed to process flow");
					}
				}
			}

			// Update consumer versions
			{
				let consumers = self.consumers.read();
				for consumer in consumers.values() {
					consumer.set_version(version);
				}
			}
		}

		// Commit FlowTransaction if we created one
		if let Some(mut ft) = flow_txn {
			ft.commit(txn)?;
		}

		Ok(())
	}
}

impl FlowLoopConsumer {
	/// Detect new flow registrations from CDC.
	fn detect_new_flows(&self, txn: &mut StandardCommandTransaction, cdc: &Cdc) -> Result<Vec<(FlowId, Flow)>> {
		let mut new_flows = Vec::new();

		for change in &cdc.changes {
			if let Some(kind) = Key::kind(change.key()) {
				if kind == KeyKind::Flow {
					if let CdcChange::Insert {
						key,
						..
					} = &change.change
					{
						if let Some(Key::Flow(flow_key)) = Key::decode(key) {
							let flow_id = flow_key.flow;

							// Check if not already registered
							if !self.flow_engine.inner.flows.read().contains_key(&flow_id) {
								// Load flow from catalog
								let flow = load_flow(txn, flow_id)?;

								// Register with flow engine
								self.flow_engine.register(txn, flow.clone())?;

								debug!(flow_id = flow_id.0, "detected new flow");
								new_flows.push((flow_id, flow));
							}
						}
					}
				}
			}
		}

		Ok(new_flows)
	}

	/// Decode CDC to FlowChanges (done once per version).
	fn decode_cdc(&self, cdc: &Cdc, version: CommitVersion) -> Result<DecodedChanges> {
		let mut changes = Vec::new();
		let mut primitives = HashSet::new();

		// Create query transaction at this version for decoding
		let mut query_txn = self.engine.begin_query_at_version(version)?;

		for cdc_change in &cdc.changes {
			if let Some(Key::Row(row_key)) = Key::decode(cdc_change.key()) {
				let source_id = row_key.primitive;
				let row_number = row_key.row;

				primitives.insert(source_id);

				// Skip Delete events with no pre-image
				if let CdcChange::Delete {
					pre: None,
					..
				} = &cdc_change.change
				{
					continue;
				}

				match convert_cdc_to_flow_change(
					&mut query_txn,
					&self.catalog_cache,
					source_id,
					row_number,
					&cdc_change.change,
					version,
				) {
					Ok(change) => changes.push(change),
					Err(e) => {
						warn!(
							source = ?source_id,
							row = row_number.0,
							error = %e,
							"failed to decode CDC change"
						);
					}
				}
			}
		}

		Ok(DecodedChanges {
			changes,
			primitives,
		})
	}

	/// Backfill a new flow from version 0 to current version.
	fn backfill_flow(
		&self,
		_parent_txn: &mut StandardCommandTransaction,
		flow_id: FlowId,
		flow: &Flow,
		up_to_version: CommitVersion,
	) -> Result<()> {
		// Skip backfill if up_to_version is 0 (nothing to backfill)
		if up_to_version.0 == 0 {
			info!(flow_id = flow_id.0, "no backfill needed (version 0)");
			return Ok(());
		}

		info!(flow_id = flow_id.0, up_to_version = up_to_version.0, "backfilling flow");

		let consumer = FlowConsumer::new(flow_id, flow.clone());
		let catalog = self.engine.catalog();

		// Fetch all CDC from version 1 to up_to_version
		let mut txn = self.engine.begin_command()?;
		let cdc_txn = txn.begin_cdc_query()?;
		let batch = cdc_txn.range(Bound::Excluded(CommitVersion(0)), Bound::Included(up_to_version))?;

		// Use a single FlowTransaction for all versions.
		// This ensures state from version N is visible to version N+1
		// because reads check self.pending first (which accumulates all writes).
		// We use update_version() to change the primitive_query snapshot for each version.
		let mut flow_txn: Option<FlowTransaction> = None;

		for cdc in batch.items {
			let version = cdc.version;

			// Decode CDC
			let decoded = self.decode_cdc(&cdc, version)?;

			// Skip if no relevant changes
			if !consumer.has_sources(&decoded.primitives) {
				continue;
			}

			// Create or update the FlowTransaction
			match &mut flow_txn {
				None => {
					flow_txn = Some(FlowTransaction::new(&mut txn, version, catalog.clone()));
				}
				Some(ft) => {
					ft.update_version(version);
				}
			}

			// Process through flow
			consumer.process(flow_txn.as_mut().unwrap(), &self.flow_engine, &decoded.changes)?;
		}

		// Commit FlowTransaction if we created one
		if let Some(mut ft) = flow_txn {
			ft.commit(&mut txn)?;
		}

		// Persist per-flow checkpoint
		let flow_consumer_id = CdcConsumerId::new(&format!("flow-consumer-{}", flow_id.0));
		CdcCheckpoint::persist(&mut txn, &flow_consumer_id, up_to_version)?;

		txn.commit()?;

		info!(flow_id = flow_id.0, "backfill complete");

		Ok(())
	}
}
