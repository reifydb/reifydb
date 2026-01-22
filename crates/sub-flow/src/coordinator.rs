// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow coordinator wrapper that provides CdcConsume interface over the CoordinatorActor.
//!
//! This module provides [`FlowCoordinator`] which implements the `CdcConsume` trait
//! while using the actor model internally for state management.

use std::sync::Arc;

use crossbeam_channel::bounded;
use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, consumer::CdcConsume},
	storage::CdcStore,
};
use reifydb_core::internal;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::{
	mailbox::ActorRef,
	runtime::{ActorHandle, ActorRuntime},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::{Result, error::Error};
use tracing::{Span, instrument};

use crate::{
	FlowEngine,
	actor::{FlowActor, FlowMsg},
	catalog::FlowCatalog,
	coordinator_actor::{CoordinatorActor, CoordinatorMsg, CoordinatorResponse, extract_new_flow_ids},
	pool::{PoolActor, PoolMsg},
	tracker::PrimitiveVersionTracker,
	transaction::pending::Pending,
};

/// Flow coordinator that implements CDC consumption logic.
///
/// Provides a synchronous CdcConsume interface by embedding reply channels
/// in actor messages and blocking on the response.
pub(crate) struct FlowCoordinator {
	catalog: FlowCatalog,
	actor_ref: ActorRef<CoordinatorMsg>,
	/// Worker handles for proper cleanup - must be joined on shutdown
	worker_handles: Vec<ActorHandle<FlowMsg>>,
	/// Pool handle for proper cleanup - must be joined on shutdown
	pool_handle: Option<ActorHandle<PoolMsg>>,
	/// Coordinator handle for proper cleanup - must be joined on shutdown
	coordinator_handle: Option<ActorHandle<CoordinatorMsg>>,
	runtime: ActorRuntime,
}

impl FlowCoordinator {
	/// Create a new flow coordinator with the given configuration.
	///
	/// Spawns worker actors, pool actor, and coordinator actor on the provided runtime.
	/// The runtime should be shared with other components (like PollConsumer) so that
	/// all actors can communicate properly, especially in WASM where actors on different
	/// runtimes cannot exchange messages.
	pub fn new<F, Fac>(
		engine: StandardEngine,
		tracker: Arc<PrimitiveVersionTracker>,
		num_workers: usize,
		factory_builder: F,
		cdc_store: CdcStore,
		runtime: ActorRuntime,
	) -> Self
	where
		F: Fn() -> Fac + Send + 'static,
		Fac: FnOnce() -> FlowEngine + Send + 'static,
	{
		let catalog = FlowCatalog::new(engine.catalog());

		let mut worker_refs = Vec::with_capacity(num_workers);
		let mut worker_handles = Vec::with_capacity(num_workers);
		for i in 0..num_workers {
			let worker_factory = factory_builder();
			let actor = FlowActor::new(worker_factory, engine.clone(), engine.catalog());
			let handle = runtime.spawn(&format!("flow-worker-{}", i), actor);
			worker_refs.push(handle.actor_ref.clone());
			worker_handles.push(handle);
		}

		let pool_actor = PoolActor::new(worker_refs);
		let pool_handle = runtime.spawn("flow-pool", pool_actor);
		let pool_ref = pool_handle.actor_ref.clone();

		let coordinator_actor = CoordinatorActor::new(
			engine.clone(),
			catalog.clone(),
			pool_ref,
			tracker,
			cdc_store,
			num_workers,
		);

		let coordinator_handle = runtime.spawn("flow-coordinator", coordinator_actor);
		let actor_ref = coordinator_handle.actor_ref.clone();

		Self {
			catalog,
			actor_ref,
			worker_handles,
			pool_handle: Some(pool_handle),
			coordinator_handle: Some(coordinator_handle),
			runtime,
		}
	}

	/// Get the parent transaction's snapshot version for state reads.
	fn get_parent_snapshot_version(
		&self,
		txn: &StandardCommandTransaction,
	) -> Result<reifydb_core::common::CommitVersion> {
		let query_txn = txn.multi.begin_query()?;
		Ok(query_txn.version())
	}

	/// Stop the coordinator and all workers.
	///
	/// This properly joins all actor threads in the correct order:
	/// 1. Signal shutdown to all actors via cancellation token
	/// 2. Join coordinator first (it sends messages to pool and workers)
	/// 3. Join pool (it sends messages to workers)
	/// 4. Join workers last
	pub fn stop(&mut self) {
		// Signal shutdown to all actors
		self.runtime.shutdown();

		// Join coordinator first (it uses pool and workers)
		if let Some(handle) = self.coordinator_handle.take() {
			let _ = handle.join();
		}

		// Join pool (it uses workers)
		if let Some(handle) = self.pool_handle.take() {
			let _ = handle.join();
		}

		// Join workers last
		for handle in self.worker_handles.drain(..) {
			let _ = handle.join();
		}
	}

	/// Get a clone of the flow catalog.
	pub fn catalog(&self) -> FlowCatalog {
		self.catalog.clone()
	}
}

impl CdcConsume for FlowCoordinator {
	#[instrument(name = "flow::coordinator::consume", level = "debug", skip(self, txn, cdcs), fields(
		cdc_count = cdcs.len(),
		version_start = tracing::field::Empty,
		version_end = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	fn consume(
		&self,
		txn: &mut StandardCommandTransaction,
		cdcs: Vec<reifydb_core::interface::cdc::Cdc>,
	) -> Result<()> {
		let consume_start = reifydb_runtime::time::Instant::now();

		// Record version range
		if let Some(first) = cdcs.first() {
			Span::current().record("version_start", first.version.0);
		}
		if let Some(last) = cdcs.last() {
			Span::current().record("version_end", last.version.0);
		}

		let state_version = self.get_parent_snapshot_version(txn)?;
		let current_version = cdcs.last().map(|c| c.version).unwrap_or(state_version);

		// Extract new flow IDs from CDC and load them from catalog
		let new_flow_ids = extract_new_flow_ids(&cdcs);
		let mut new_flows = Vec::with_capacity(new_flow_ids.len());
		for flow_id in new_flow_ids {
			let (flow, is_new) = self.catalog.get_or_load_flow(txn, flow_id)?;
			if is_new {
				new_flows.push(flow);
			}
		}

		// Send to coordinator actor
		let (reply_tx, reply_rx) = bounded(1);

		self.actor_ref
			.send(CoordinatorMsg::Consume {
				cdcs,
				state_version,
				new_flows,
				current_version,
				reply: reply_tx,
			})
			.map_err(|_| Error(internal!("Coordinator actor stopped")))?;

		let response = reply_rx.recv().map_err(|_| Error(internal!("Coordinator actor response error")))?;

		// Apply results to transaction
		match response {
			CoordinatorResponse::Success(result) => {
				// Apply pending writes
				for (key, pending) in result.pending_writes.iter_sorted() {
					match pending {
						Pending::Set(value) => {
							txn.set(key, value.clone())?;
						}
						Pending::Remove => {
							txn.remove(key)?;
						}
					}
				}

				// Persist checkpoints
				for (flow_id, version) in result.checkpoints {
					CdcCheckpoint::persist(txn, &flow_id, version)?;
				}
			}
			CoordinatorResponse::Error(e) => {
				return Err(Error(internal!("{}", e)));
			}
		}

		Span::current().record("elapsed_us", consume_start.elapsed().as_micros() as u64);
		Ok(())
	}
}

impl Drop for FlowCoordinator {
	fn drop(&mut self) {
		self.stop();
	}
}
