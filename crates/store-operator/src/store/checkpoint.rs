// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use tracing::instrument;

use crate::store::{OperatorStore, StandardOperatorStore};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::checkpoint_set", level = "debug", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) {
		self.commit.record_checkpoint_set(flow, version);
	}

	#[instrument(name = "store::operator::checkpoint_delete", level = "debug", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_delete(&self, flow: FlowId) {
		self.commit.record_checkpoint_delete(flow);
	}

	#[instrument(name = "store::operator::checkpoint_get", level = "trace", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_get(&self, flow: FlowId) -> Option<CommitVersion> {
		if let Some(entry) = self.commit.lookup_checkpoint(flow) {
			return entry;
		}
		self.persistent.as_ref()?.checkpoint_get(flow)
	}

	#[instrument(name = "store::operator::checkpoint_floor", level = "trace", skip(self))]
	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		let durable = self.persistent.as_ref().and_then(|persistent| persistent.checkpoint_floor());
		match (durable, self.commit.checkpoint_floor()) {
			(Some(durable), Some(buffered)) => Some(durable.min(buffered)),
			(durable, buffered) => durable.or(buffered),
		}
	}

	#[instrument(name = "store::operator::checkpoint_list", level = "trace", skip(self))]
	pub fn checkpoint_list(&self) -> Vec<FlowId> {
		let mut merged: BTreeSet<FlowId> = self
			.persistent
			.as_ref()
			.map(|persistent| persistent.checkpoint_list())
			.unwrap_or_default()
			.into_iter()
			.collect();
		for (flow, entry) in self.commit.checkpoint_entries() {
			match entry {
				Some(_) => merged.insert(flow),
				None => merged.remove(&flow),
			};
		}
		merged.into_iter().collect()
	}
}

impl OperatorStore {
	pub fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) {
		match self {
			Self::Standard(store) => store.checkpoint_set(flow, version),
		}
	}

	pub fn checkpoint_delete(&self, flow: FlowId) {
		match self {
			Self::Standard(store) => store.checkpoint_delete(flow),
		}
	}

	pub fn checkpoint_get(&self, flow: FlowId) -> Option<CommitVersion> {
		match self {
			Self::Standard(store) => store.checkpoint_get(flow),
		}
	}

	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		match self {
			Self::Standard(store) => store.checkpoint_floor(),
		}
	}

	pub fn checkpoint_list(&self) -> Vec<FlowId> {
		match self {
			Self::Standard(store) => store.checkpoint_list(),
		}
	}
}
