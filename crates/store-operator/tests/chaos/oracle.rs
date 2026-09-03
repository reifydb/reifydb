// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The reference model: what every read must return, independent of which tier serves it. The operator store is
//! single-version, so unlike the multi-store oracle this one is exact truth at every step, with no watermark
//! below which history may legally be collapsed.

use std::{collections::BTreeMap, ops::Bound};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::key::operator::state::OperatorStateKey;
use reifydb_value::byte_size::ByteSize;

type StateKey = (u64, Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CensusRow {
	pub operator: u64,
	pub keyspace: u8,
	pub keys: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
}

#[derive(Clone, Default)]
pub struct Oracle {
	state: BTreeMap<StateKey, EncodedPodRow>,
	checkpoints: BTreeMap<u64, u64>,
}

impl Oracle {
	pub fn set(&mut self, operator: u64, key: &[u8], row: EncodedPodRow) {
		self.state.insert((operator, key.to_vec()), row);
	}

	pub fn remove(&mut self, operator: u64, key: &[u8]) {
		self.state.remove(&(operator, key.to_vec()));
	}

	pub fn drop_operator_state(&mut self, operator: u64) {
		self.state.retain(|(candidate, _), _| *candidate != operator);
	}

	pub fn checkpoint_set(&mut self, flow: u64, version: u64) {
		self.checkpoints.insert(flow, version);
	}

	pub fn checkpoint_delete(&mut self, flow: u64) {
		self.checkpoints.remove(&flow);
	}

	pub fn get(&self, operator: u64, key: &[u8]) -> Option<Vec<u8>> {
		self.state.get(&(operator, key.to_vec())).map(|row| row.body().to_vec())
	}

	/// The size the census bills and a batch claim is checked against: the whole encoded row, not `get`'s body.
	pub fn value_bytes(&self, operator: u64, key: &[u8]) -> Option<ByteSize> {
		self.state.get(&(operator, key.to_vec())).map(|row| ByteSize::from_bytes(row.bytes().len() as u64))
	}

	pub fn contains(&self, operator: u64, key: &[u8]) -> bool {
		self.state.contains_key(&(operator, key.to_vec()))
	}

	pub fn state_len(&self) -> usize {
		self.state.len()
	}

	/// Indexed sampling over the live keys. The slot space dwarfs a batch, so a generator that only draws fresh
	/// keys never reaches the present-key arithmetic; aiming a write here is what makes replace and remove of a
	/// held key happen at all.
	pub fn nth_state_slot(&self, index: usize) -> Option<(u64, Vec<u8>)> {
		self.state.keys().nth(index).cloned()
	}

	pub fn checkpoint_get(&self, flow: u64) -> Option<u64> {
		self.checkpoints.get(&flow).copied()
	}

	pub fn checkpoint_list(&self) -> Vec<u64> {
		self.checkpoints.keys().copied().collect()
	}

	/// Every key of `operator` inside the bounds, in encoded-key order, as (key, body). Callers apply the
	/// batch limit themselves so they can also derive `has_more`.
	pub fn range(&self, operator: u64, start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
		self.state
			.iter()
			.filter(|((candidate, _), _)| *candidate == operator)
			.filter(|((_, key), _)| in_bounds(key, start, end))
			.map(|((_, key), row)| (key.clone(), row.body().to_vec()))
			.collect()
	}

	/// Census buckets in the order every tier emits them: by operator, then by the ENCODED keyspace byte, which
	/// is the byte actually stored in the key and indexed by sqlite. A key is billed for its group and suffix
	/// only: the keyspace byte names the table the row lands in and costs nothing per row.
	pub fn census(&self) -> Vec<CensusRow> {
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		let mut buckets: BTreeMap<(u64, u8), CensusRow> = BTreeMap::new();
		for ((operator, key), row) in &self.state {
			let stored = *key.get(offset).expect("state keys carry a keyspace byte");
			let bucket = buckets.entry((*operator, stored)).or_insert(CensusRow {
				operator: *operator,
				keyspace: OperatorStateKey::decode_keyspace(stored).0,
				keys: 0,
				key_bytes: 0,
				value_bytes: 0,
			});
			bucket.keys += 1;
			bucket.key_bytes += (key.len() - 1) as u64;
			bucket.value_bytes += row.bytes().len() as u64;
		}
		buckets.into_values().collect()
	}

	pub fn bytes(&self, operator: u64) -> u64 {
		self.state
			.iter()
			.filter(|((candidate, _), _)| *candidate == operator)
			.map(|((_, key), row)| (key.len() - 1 + row.bytes().len()) as u64)
			.sum()
	}

	pub fn total_bytes(&self) -> u64 {
		self.state.iter().map(|((_, key), row)| (key.len() - 1 + row.bytes().len()) as u64).sum()
	}
}

fn in_bounds(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
	let lower = match start {
		Bound::Included(bound) => key >= bound.as_slice(),
		Bound::Excluded(bound) => key > bound.as_slice(),
		Bound::Unbounded => true,
	};
	let upper = match end {
		Bound::Included(bound) => key <= bound.as_slice(),
		Bound::Excluded(bound) => key < bound.as_slice(),
		Bound::Unbounded => true,
	};
	lower && upper
}

/// What `checkpoint_floor` must report on one configuration. The layered tier deliberately reports
/// `min(durable, buffered)` rather than the logical minimum, so retention never reaps a version a restart would
/// send the flow back to; a write-through tier has no buffer and so reports the logical minimum.
#[derive(Default)]
pub struct CheckpointModel {
	durable: BTreeMap<u64, u64>,
	pending: BTreeMap<u64, Option<u64>>,
	write_through: bool,
}

impl CheckpointModel {
	pub fn new(write_through: bool) -> Self {
		Self {
			durable: BTreeMap::new(),
			pending: BTreeMap::new(),
			write_through,
		}
	}

	pub fn set(&mut self, flow: u64, version: u64) {
		match self.write_through {
			true => {
				self.durable.insert(flow, version);
			}
			false => {
				self.pending.insert(flow, Some(version));
			}
		}
	}

	pub fn delete(&mut self, flow: u64) {
		match self.write_through {
			true => {
				self.durable.remove(&flow);
			}
			false => {
				self.pending.insert(flow, None);
			}
		}
	}

	pub fn flush(&mut self) {
		for (flow, entry) in std::mem::take(&mut self.pending) {
			match entry {
				Some(version) => self.durable.insert(flow, version),
				None => self.durable.remove(&flow),
			};
		}
	}

	pub fn floor(&self) -> Option<u64> {
		let durable = self.durable.values().copied().min();
		let buffered = self.pending.values().flatten().copied().min();
		match (durable, buffered) {
			(Some(durable), Some(buffered)) => Some(durable.min(buffered)),
			(durable, buffered) => durable.or(buffered),
		}
	}
}
