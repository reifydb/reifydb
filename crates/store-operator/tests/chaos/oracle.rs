// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The reference model: what every read must return, independent of which tier serves it. The operator store is
//! single-version, so unlike the multi-store oracle this one is exact truth at every step, with no watermark
//! below which history may legally be collapsed.

use std::{collections::BTreeMap, ops::Bound};

use reifydb_codec::row::operator::EncodedOperatorRow;
use reifydb_core::key::operator_state::OperatorStateKey;
use reifydb_store_operator::types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES};

type StateKey = (u64, Vec<u8>);

type AnchorKey = (u64, u64, u8, u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CensusRow {
	pub operator: u64,
	pub keyspace: u8,
	pub keys: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchorRow {
	pub expiry: u64,
	pub side: u8,
	pub row_number: u64,
}

#[derive(Clone, Default)]
pub struct Oracle {
	state: BTreeMap<StateKey, EncodedOperatorRow>,
	anchors: BTreeMap<AnchorKey, u64>,
	checkpoints: BTreeMap<u64, u64>,
}

impl Oracle {
	pub fn set(&mut self, operator: u64, key: &[u8], row: EncodedOperatorRow) {
		self.state.insert((operator, key.to_vec()), row);
	}

	pub fn remove(&mut self, operator: u64, key: &[u8]) {
		self.state.remove(&(operator, key.to_vec()));
	}

	pub fn drop_operator_state(&mut self, operator: u64) {
		self.state.retain(|(candidate, _), _| *candidate != operator);
		self.anchors.retain(|(candidate, _, _, _), _| *candidate != operator);
	}

	pub fn anchor_set(&mut self, operator: u64, group: u64, side: u8, row_number: u64, expiry: u64) {
		self.anchors.insert((operator, group, side, row_number), expiry);
	}

	pub fn anchor_remove(&mut self, operator: u64, group: u64, side: u8, row_number: u64) {
		self.anchors.remove(&(operator, group, side, row_number));
	}

	pub fn anchors_remove_group(&mut self, operator: u64, group: u64) {
		self.anchors.retain(|(candidate, candidate_group, _, _), _| {
			*candidate != operator || *candidate_group != group
		});
	}

	pub fn anchors_drop_operator(&mut self, operator: u64) {
		self.anchors.retain(|(candidate, _, _, _), _| *candidate != operator);
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

	pub fn contains(&self, operator: u64, key: &[u8]) -> bool {
		self.state.contains_key(&(operator, key.to_vec()))
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
	/// is the byte actually stored in the key and indexed by sqlite.
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
			bucket.key_bytes += key.len() as u64;
			bucket.value_bytes += row.bytes().len() as u64;
		}
		buckets.into_values().collect()
	}

	pub fn anchor_census(&self) -> Vec<(u64, u64)> {
		let mut buckets: BTreeMap<u64, u64> = BTreeMap::new();
		for (operator, _, _, _) in self.anchors.keys() {
			*buckets.entry(*operator).or_insert(0) += 1;
		}
		buckets.into_iter().collect()
	}

	pub fn bytes(&self, operator: u64) -> u64 {
		let state: u64 = self
			.state
			.iter()
			.filter(|((candidate, _), _)| *candidate == operator)
			.map(|((_, key), row)| (key.len() + row.bytes().len()) as u64)
			.sum();
		let anchors = self.anchors.keys().filter(|(candidate, _, _, _)| *candidate == operator).count() as u64;
		state + anchor_bytes(anchors)
	}

	pub fn total_bytes(&self) -> u64 {
		let state: u64 = self.state.iter().map(|((_, key), row)| (key.len() + row.bytes().len()) as u64).sum();
		state + anchor_bytes(self.anchors.len() as u64)
	}

	pub fn anchor_get(&self, operator: u64, group: u64, side: u8, row_number: u64) -> Option<u64> {
		self.anchors.get(&(operator, group, side, row_number)).copied()
	}

	/// Every anchor of the group that qualifies, ordered by expiry then by slot. A tier is free to break expiry
	/// ties differently, so callers compare expiries positionally and slots as a set.
	pub fn eligible_anchors(&self, operator: u64, group: u64, due: Option<u64>) -> Vec<AnchorRow> {
		let mut rows: Vec<AnchorRow> = self
			.anchors
			.iter()
			.filter(|((candidate, candidate_group, _, _), _)| {
				*candidate == operator && *candidate_group == group
			})
			.filter(|(_, expiry)| due.is_none_or(|at| **expiry <= at))
			.map(|((_, _, side, row_number), expiry)| AnchorRow {
				expiry: *expiry,
				side: *side,
				row_number: *row_number,
			})
			.collect();
		rows.sort_by_key(|row| (row.expiry, row.side, row.row_number));
		rows
	}
}

fn anchor_bytes(count: u64) -> u64 {
	(ANCHOR_KEY_BYTES.as_bytes() + ANCHOR_VALUE_BYTES.as_bytes()) * count
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
