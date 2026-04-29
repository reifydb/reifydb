// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{Arc, RwLock, RwLockReadGuard},
};

use dashmap::DashMap;
use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::consume::consumer::CdcConsume;
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
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
		let Some(flow_engine) = self.acquire_flow_engine() else {
			reply(Ok(()));
			return;
		};
		if flow_engine.sources.is_empty() {
			reply(Ok(()));
			return;
		}

		self.process_cdc_batch(&flow_engine, &cdcs);

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

impl SubscriptionCdcConsumer {
	#[inline]
	fn acquire_flow_engine(&self) -> Option<RwLockReadGuard<'_, FlowEngine>> {
		self.flow_engine.read().ok()
	}

	fn process_cdc_batch(&self, flow_engine: &FlowEngine, cdcs: &[Cdc]) {
		for cdc in cdcs {
			let version = cdc.version;
			for change in &cdc.changes {
				let source_shape = match &change.origin {
					ChangeOrigin::Shape(s) => *s,
					ChangeOrigin::Flow(_) => continue,
				};
				let Some(flow_entries) = flow_engine.sources.get(&source_shape).cloned() else {
					continue;
				};
				self.process_change_for_flows(flow_engine, version, change, &flow_entries);
			}
		}
	}

	fn process_change_for_flows(
		&self,
		flow_engine: &FlowEngine,
		version: CommitVersion,
		change: &Change,
		flow_entries: &[(FlowId, FlowNodeId)],
	) {
		for (flow_id, node_id) in flow_entries {
			let state = self.flow_states.remove(flow_id).map(|(_, v)| v).unwrap_or_default();
			let Ok(primitive_query) = self.multi.begin_query() else {
				continue;
			};
			let mut txn = FlowTransaction::ephemeral(
				version,
				primitive_query,
				self.catalog.clone(),
				state,
				flow_engine.clock().clone(),
			);
			let flow_change = Change::from_flow(*node_id, version, change.diffs.clone(), change.changed_at);
			if flow_engine.process(&mut txn, flow_change, *flow_id).is_ok() {
				let _ = txn.flush_operator_states();
				txn.release_ffi_scratch();
				txn.merge_state();
			}
			self.flow_states.insert(*flow_id, txn.take_state());
		}
	}
}
