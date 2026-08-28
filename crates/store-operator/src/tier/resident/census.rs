// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, btree_map::Entry};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::OperatorStateKey};
use reifydb_value::byte_size::ByteSize;

use crate::{
	tier::resident::{BufferInner, OperatorResidentState},
	types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, OperatorSealAnchorCensus, OperatorStateCensus},
};

type BucketId = (OperatorId, Option<u8>);

type StateBuckets = BTreeMap<BucketId, StateBucket>;

type AnchorBuckets = BTreeMap<OperatorId, u64>;

#[derive(Debug, Default, Clone, Copy)]
struct StateBucket {
	keys: u64,
	key_bytes: ByteSize,
	value_bytes: ByteSize,
}

#[derive(Debug, Default)]
pub(super) struct BufferCensus {
	state: StateBuckets,
	anchors: AnchorBuckets,
}

impl BufferCensus {
	pub(super) fn admit_state(&mut self, operator: OperatorId, key: &EncodedKey, value_bytes: u64) {
		admit_state(&mut self.state, operator, key, value_bytes);
	}

	pub(super) fn retract_state(&mut self, operator: OperatorId, key: &EncodedKey, value_bytes: u64) {
		let Entry::Occupied(mut slot) = self.state.entry(bucket_id(operator, key)) else {
			return;
		};
		let bucket = slot.get_mut();
		bucket.keys = bucket.keys.saturating_sub(1);
		bucket.key_bytes = bucket.key_bytes.saturating_sub(ByteSize::from_bytes(key.len() as u64));
		bucket.value_bytes = bucket.value_bytes.saturating_sub(ByteSize::from_bytes(value_bytes));
		if bucket.keys == 0 {
			slot.remove();
		}
	}

	pub(super) fn admit_anchor(&mut self, operator: OperatorId) {
		*self.anchors.entry(operator).or_insert(0) += 1;
	}

	pub(super) fn retract_anchor(&mut self, operator: OperatorId) {
		let Entry::Occupied(mut slot) = self.anchors.entry(operator) else {
			return;
		};
		let remaining = slot.get().saturating_sub(1);
		if remaining == 0 {
			slot.remove();
			return;
		}
		*slot.get_mut() = remaining;
	}
}

pub(super) fn release_in_flight(inner: &mut BufferInner) {
	let Some(batch) = inner.in_flight.take() else {
		return;
	};
	let BufferInner {
		live,
		census,
		..
	} = inner;
	for ((operator, key), entry) in batch.state.iter() {
		if live.state.lookup(operator, key).is_some() {
			continue;
		}
		if let Some(row) = &entry.post {
			census.retract_state(operator, key, row.bytes().len() as u64);
		}
	}
	for (key, entry) in &batch.anchors {
		if live.anchors.contains_key(key) {
			continue;
		}
		if entry.is_some() {
			census.retract_anchor(key.0);
		}
	}
}

fn bucket_id(operator: OperatorId, key: &EncodedKey) -> BucketId {
	(operator, key.as_slice().get(OperatorStateKey::KEYSPACE_INNER_OFFSET as usize).copied())
}

fn admit_state(buckets: &mut StateBuckets, operator: OperatorId, key: &EncodedKey, value_bytes: u64) {
	let bucket = buckets.entry(bucket_id(operator, key)).or_default();
	bucket.keys += 1;
	bucket.key_bytes = bucket.key_bytes.saturating_add(ByteSize::from_bytes(key.len() as u64));
	bucket.value_bytes = bucket.value_bytes.saturating_add(ByteSize::from_bytes(value_bytes));
}

fn scan_state(inner: &BufferInner, mut visit: impl FnMut(OperatorId, &EncodedKey, &EncodedPodRow)) {
	for ((operator, key), entry) in inner.live.state.iter() {
		if let Some(row) = &entry.post {
			visit(operator, key, row);
		}
	}
	let Some(batch) = inner.in_flight.as_deref() else {
		return;
	};
	for ((operator, key), entry) in batch.state.iter() {
		if inner.live.state.lookup(operator, key).is_some() {
			continue;
		}
		if let Some(row) = &entry.post {
			visit(operator, key, row);
		}
	}
}

fn scan_anchors(inner: &BufferInner, mut visit: impl FnMut(OperatorId)) {
	for (key, entry) in &inner.live.anchors {
		if entry.is_some() {
			visit(key.0);
		}
	}
	let Some(batch) = inner.in_flight.as_deref() else {
		return;
	};
	for (key, entry) in &batch.anchors {
		if inner.live.anchors.contains_key(key) {
			continue;
		}
		if entry.is_some() {
			visit(key.0);
		}
	}
}

fn scanned_state(inner: &BufferInner) -> StateBuckets {
	let mut buckets = StateBuckets::new();
	scan_state(inner, |operator, key, row| {
		admit_state(&mut buckets, operator, key, row.bytes().len() as u64);
	});
	buckets
}

fn scanned_anchors(inner: &BufferInner) -> AnchorBuckets {
	let mut buckets = AnchorBuckets::new();
	scan_anchors(inner, |operator| {
		*buckets.entry(operator).or_insert(0) += 1;
	});
	buckets
}

fn state_census(buckets: &StateBuckets) -> Vec<OperatorStateCensus> {
	buckets.iter()
		.map(|((operator, stored), bucket)| {
			let stored = stored.expect("state keys carry a keyspace byte");
			OperatorStateCensus {
				operator: *operator,
				keyspace: OperatorStateKey::decode_keyspace(stored),
				keys: bucket.keys,
				key_bytes: bucket.key_bytes,
				value_bytes: bucket.value_bytes,
			}
		})
		.collect()
}

fn anchor_census(buckets: &AnchorBuckets) -> Vec<OperatorSealAnchorCensus> {
	buckets.iter()
		.map(|(operator, keys)| OperatorSealAnchorCensus {
			operator: *operator,
			keys: *keys,
		})
		.collect()
}

fn anchor_bytes(anchors: u64) -> ByteSize {
	(ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES) * anchors
}

impl OperatorResidentState {
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let inner = self.shared().inner.lock();
		let mut total = ByteSize::ZERO;
		scan_state(&inner, |candidate, key, row| {
			if candidate != operator {
				return;
			}
			total = total
				.saturating_add(ByteSize::from_bytes(key.len() as u64 + row.bytes().len() as u64));
		});
		let mut anchors = 0u64;
		scan_anchors(&inner, |candidate| {
			if candidate == operator {
				anchors += 1;
			}
		});
		total.saturating_add(anchor_bytes(anchors))
	}

	pub fn total_bytes(&self) -> ByteSize {
		let inner = self.shared().inner.lock();
		let mut total = ByteSize::ZERO;
		scan_state(&inner, |_, key, row| {
			total = total
				.saturating_add(ByteSize::from_bytes(key.len() as u64 + row.bytes().len() as u64));
		});
		let mut anchors = 0u64;
		scan_anchors(&inner, |_| {
			anchors += 1;
		});
		total.saturating_add(anchor_bytes(anchors))
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		state_census(&self.shared().inner.lock().census.state)
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		anchor_census(&self.shared().inner.lock().census.anchors)
	}

	pub fn census_by_scan(&self) -> Vec<OperatorStateCensus> {
		state_census(&scanned_state(&self.shared().inner.lock()))
	}

	pub fn anchor_census_by_scan(&self) -> Vec<OperatorSealAnchorCensus> {
		anchor_census(&scanned_anchors(&self.shared().inner.lock()))
	}
}
