// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::OperatorStateKey};
use reifydb_value::byte_size::ByteSize;

use crate::{
	tier::resident::{
		OperatorResidentState,
		batch::{AnchorKey, FlushBatch},
		resident,
	},
	types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, OperatorSealAnchorCensus, OperatorStateCensus},
};

fn live_state<'a>(
	batches: impl Iterator<Item = &'a FlushBatch>,
) -> BTreeMap<(OperatorId, &'a EncodedKey), &'a EncodedPodRow> {
	let mut view = BTreeMap::new();
	for batch in batches {
		for (key, entry) in &batch.state {
			match &entry.post {
				Some(row) => view.insert(key, row),
				None => view.remove(&key),
			};
		}
	}
	view
}

fn live_anchors<'a>(batches: impl Iterator<Item = &'a FlushBatch>) -> BTreeMap<&'a AnchorKey, u64> {
	let mut view = BTreeMap::new();
	for batch in batches {
		for (id, entry) in &batch.anchors {
			match entry {
				Some(expiry) => view.insert(id, *expiry),
				None => view.remove(id),
			};
		}
	}
	view
}

fn anchor_bytes(anchors: u64) -> ByteSize {
	(ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES) * anchors
}

impl OperatorResidentState {
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let inner = self.shared().inner.lock();
		let mut total = ByteSize::ZERO;
		for ((_, key), row) in live_state(resident(&inner)).iter().filter(|((op, _), _)| *op == operator) {
			total = total.saturating_add(ByteSize::from_bytes(key.len() as u64 + row.bytes().len() as u64));
		}
		let anchors =
			live_anchors(resident(&inner)).keys().filter(|(op, _, _, _)| *op == operator).count() as u64;
		total.saturating_add(anchor_bytes(anchors))
	}

	pub fn total_bytes(&self) -> ByteSize {
		let inner = self.shared().inner.lock();
		let mut total = ByteSize::ZERO;
		for ((_, key), row) in live_state(resident(&inner)) {
			total = total.saturating_add(ByteSize::from_bytes(key.len() as u64 + row.bytes().len() as u64));
		}
		total.saturating_add(anchor_bytes(live_anchors(resident(&inner)).len() as u64))
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let inner = self.shared().inner.lock();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		let mut buckets: BTreeMap<(OperatorId, u8), OperatorStateCensus> = BTreeMap::new();
		for ((operator, key), row) in live_state(resident(&inner)) {
			let stored = *key.as_slice().get(offset).expect("state keys carry a keyspace byte");
			let bucket = buckets.entry((operator, stored)).or_insert(OperatorStateCensus {
				operator,
				keyspace: OperatorStateKey::decode_keyspace(stored),
				keys: 0,
				key_bytes: ByteSize::ZERO,
				value_bytes: ByteSize::ZERO,
			});
			bucket.keys += 1;
			bucket.key_bytes = bucket.key_bytes.saturating_add(ByteSize::from_bytes(key.len() as u64));
			bucket.value_bytes =
				bucket.value_bytes.saturating_add(ByteSize::from_bytes(row.bytes().len() as u64));
		}
		buckets.into_values().collect()
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		let inner = self.shared().inner.lock();
		let mut buckets: BTreeMap<OperatorId, u64> = BTreeMap::new();
		for (operator, _, _, _) in live_anchors(resident(&inner)).keys() {
			*buckets.entry(*operator).or_insert(0) += 1;
		}
		buckets.into_iter()
			.map(|(operator, keys)| OperatorSealAnchorCensus {
				operator,
				keys,
			})
			.collect()
	}
}
