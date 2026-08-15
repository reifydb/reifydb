// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::OperatorStateKey};
use reifydb_value::byte_size::ByteSize;

use crate::{
	commit::{OperatorCommitBuffer, resident},
	types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, OperatorSealAnchorCensus, OperatorStateCensus},
};

impl OperatorCommitBuffer {
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let inner = self.shared.inner.lock();
		let mut total = ByteSize::ZERO;
		for batch in resident(&inner) {
			for ((candidate, key), row) in &batch.state {
				if *candidate != operator {
					continue;
				}
				if let Some(row) = row {
					total = total.saturating_add(ByteSize::from_bytes(
						key.len() as u64 + row.bytes().len() as u64,
					));
				}
			}
			let anchors = batch
				.anchors
				.iter()
				.filter(|((candidate, _, _, _), entry)| *candidate == operator && entry.is_some())
				.count() as u64;
			total = total.saturating_add((ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES) * anchors);
		}
		total
	}

	pub fn total_bytes(&self) -> ByteSize {
		let inner = self.shared.inner.lock();
		let mut total = ByteSize::ZERO;
		for batch in resident(&inner) {
			for ((_, key), row) in &batch.state {
				if let Some(row) = row {
					total = total.saturating_add(ByteSize::from_bytes(
						key.len() as u64 + row.bytes().len() as u64,
					));
				}
			}
			let anchors = batch.anchors.values().filter(|entry| entry.is_some()).count() as u64;
			total = total.saturating_add((ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES) * anchors);
		}
		total
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let inner = self.shared.inner.lock();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		let mut buckets: BTreeMap<(OperatorId, u8), OperatorStateCensus> = BTreeMap::new();
		for batch in resident(&inner) {
			for ((operator, key), row) in &batch.state {
				let Some(row) = row else {
					continue;
				};
				let stored = *key.as_slice().get(offset).expect("state keys carry a keyspace byte");
				let bucket = buckets.entry((*operator, stored)).or_insert(OperatorStateCensus {
					operator: *operator,
					keyspace: OperatorStateKey::decode_keyspace(stored),
					keys: 0,
					key_bytes: ByteSize::ZERO,
					value_bytes: ByteSize::ZERO,
				});
				bucket.keys += 1;
				bucket.key_bytes =
					bucket.key_bytes.saturating_add(ByteSize::from_bytes(key.len() as u64));
				bucket.value_bytes = bucket
					.value_bytes
					.saturating_add(ByteSize::from_bytes(row.bytes().len() as u64));
			}
		}
		buckets.into_values().collect()
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		let inner = self.shared.inner.lock();
		let mut buckets: BTreeMap<OperatorId, u64> = BTreeMap::new();
		for batch in resident(&inner) {
			for ((operator, _, _, _), entry) in &batch.anchors {
				if entry.is_some() {
					*buckets.entry(*operator).or_insert(0) += 1;
				}
			}
		}
		buckets.into_iter()
			.map(|(operator, keys)| OperatorSealAnchorCensus {
				operator,
				keys,
			})
			.collect()
	}
}
