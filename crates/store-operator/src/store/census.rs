// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::key::encode_u8;
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	store::{OperatorStore, StandardOperatorStore},
	types::{OperatorSealAnchorCensus, OperatorStateCensus},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let durable =
			self.persistent.as_ref().map(|persistent| persistent.bytes(operator)).unwrap_or(ByteSize::ZERO);
		durable + self.commit.bytes(operator)
	}

	#[instrument(name = "store::operator::total_bytes", level = "trace", skip(self), ret)]
	pub fn total_bytes(&self) -> ByteSize {
		let durable =
			self.persistent.as_ref().map(|persistent| persistent.total_bytes()).unwrap_or(ByteSize::ZERO);
		durable + self.commit.total_bytes()
	}

	#[instrument(name = "store::operator::census", level = "debug", skip(self))]
	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let durable = self.persistent.as_ref().map(|persistent| persistent.census()).unwrap_or_default();
		let mut merged: BTreeMap<(OperatorId, u8), OperatorStateCensus> = BTreeMap::new();
		for entry in durable.into_iter().chain(self.commit.census()) {
			let stored = encode_u8(entry.keyspace.0);
			let bucket = merged.entry((entry.operator, stored)).or_insert(OperatorStateCensus {
				operator: entry.operator,
				keyspace: entry.keyspace,
				keys: 0,
				key_bytes: ByteSize::ZERO,
				value_bytes: ByteSize::ZERO,
			});
			bucket.keys += entry.keys;
			bucket.key_bytes = bucket.key_bytes.saturating_add(entry.key_bytes);
			bucket.value_bytes = bucket.value_bytes.saturating_add(entry.value_bytes);
		}
		merged.into_values().collect()
	}

	#[instrument(name = "store::operator::anchor_census", level = "debug", skip(self))]
	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		let durable = self.persistent.as_ref().map(|persistent| persistent.anchor_census()).unwrap_or_default();
		let mut merged: BTreeMap<OperatorId, u64> = BTreeMap::new();
		for entry in durable.into_iter().chain(self.commit.anchor_census()) {
			*merged.entry(entry.operator).or_insert(0) += entry.keys;
		}
		merged.into_iter()
			.map(|(operator, keys)| OperatorSealAnchorCensus {
				operator,
				keys,
			})
			.collect()
	}
}

impl OperatorStore {
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		match self {
			Self::Standard(store) => store.bytes(operator),
		}
	}

	pub fn total_bytes(&self) -> ByteSize {
		match self {
			Self::Standard(store) => store.total_bytes(),
		}
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		match self {
			Self::Standard(store) => store.census(),
		}
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		match self {
			Self::Standard(store) => store.anchor_census(),
		}
	}
}
