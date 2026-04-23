// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{Arc, RwLock},
};

use dashmap::DashMap;
use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::consume::consumer::CdcConsume;
use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{
		catalog::flow::FlowId,
		cdc::Cdc,
		change::{Change, ChangeOrigin},
	},
};
use reifydb_sub_flow::{engine::FlowEngine, transaction::FlowTransaction};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_type::Result;

use crate::sink::DeliveryBuffer;

/// CDC consumer for ephemeral subscription flows.
///
/// Processes CDC events through registered subscription flows. Sinks stage
/// their output in `delivery`; after all flows for a CDC batch have been
/// processed, the consumer commits the staged batch to the subscription store
/// atomically. This is what guarantees that the subscription poller observes
/// every batch member's diff together rather than seeing a partial batch.
pub struct SubscriptionCdcConsumer {
	flow_engine: Arc<RwLock<FlowEngine>>,
	multi: MultiTransaction,
	catalog: Catalog,
	/// Per-flow ephemeral operator state, persisted across CDC batches.
	flow_states: Arc<DashMap<FlowId, HashMap<EncodedKey, EncodedRow>>>,
	/// Staging buffer shared with every subscription sink.
	delivery: Arc<DeliveryBuffer>,
}

impl SubscriptionCdcConsumer {
	pub fn new(
		flow_engine: Arc<RwLock<FlowEngine>>,
		multi: MultiTransaction,
		catalog: Catalog,
		flow_states: Arc<DashMap<FlowId, HashMap<EncodedKey, EncodedRow>>>,
		delivery: Arc<DeliveryBuffer>,
	) -> Self {
		Self {
			flow_engine,
			multi,
			catalog,
			flow_states,
			delivery,
		}
	}
}

impl CdcConsume for SubscriptionCdcConsumer {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		let flow_engine = match self.flow_engine.read() {
			Ok(guard) => guard,
			Err(_) => {
				reply(Ok(()));
				return;
			}
		};

		// No subscription flows registered - skip processing
		if flow_engine.sources.is_empty() {
			reply(Ok(()));
			return;
		}

		for cdc in &cdcs {
			let version = cdc.version;

			for change in &cdc.changes {
				let source_shape = match &change.origin {
					ChangeOrigin::Shape(s) => *s,
					ChangeOrigin::Flow(_) => continue,
				};

				let flow_entries = match flow_engine.sources.get(&source_shape) {
					Some(entries) => entries.clone(),
					None => continue,
				};

				for (flow_id, _node_id) in &flow_entries {
					// Take ephemeral state for this flow (avoids cloning the HashMap)
					let state =
						self.flow_states.remove(flow_id).map(|(_, v)| v).unwrap_or_default();

					// Create ephemeral transaction for this flow
					let primitive_query = match self.multi.begin_query() {
						Ok(q) => q,
						Err(_) => continue,
					};

					let mut txn = FlowTransaction::ephemeral(
						version,
						primitive_query,
						self.catalog.clone(),
						state,
						flow_engine.clock().clone(),
					);

					// Process the change through the flow
					let flow_change = Change::from_flow(
						*_node_id,
						version,
						change.diffs.clone(),
						change.changed_at,
					);

					if flow_engine.process(&mut txn, flow_change, *flow_id).is_ok() {
						txn.merge_state();
					}
					// Always put state back (original on failure, merged on success)
					self.flow_states.insert(*flow_id, txn.take_state());
				}
			}
		}

		// Drop the read guard before committing: commit takes the coord write
		// lock on the store, and holding the flow_engine read lock here is
		// unnecessary for the commit itself.
		drop(flow_engine);

		// Atomically publish every diff staged by sinks during this CDC
		// batch. Without this, the poller can observe some members' diffs
		// while others are still being produced.
		self.delivery.commit_batch();

		reply(Ok(()));
	}
}
