// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Schema-agnostic windowing state-machine engines.
//!
//! Each engine owns the per-(group,window) accumulator state, high-water late
//! rejection, eviction, and diff routing (`Insert -> add`,
//! `Update -> remove(pre) + add(post)`, `Remove -> remove(pre)`). The caller
//! (the "face") owns extraction (`row -> (group, coord, contribution)`) and
//! output construction; it hands the engine pre-bucketed events and receives
//! [`WindowResult`]s to translate into diffs.

pub mod config;
pub mod multi_rolling;
pub mod rolling;
pub mod rolling_incremental;
pub mod tumbling;
pub mod tumbling_carry;

use std::ops::Bound;

use reifydb_codec::key::{
	encode_u64,
	encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey},
};
use reifydb_value::value::row_number::RowNumber;
use serde::{Deserialize, Serialize};

use crate::{key::flow_node_internal_state::FlowNodeInternalStateKey, window::span::WindowSpan};

/// One contribution routed to a window accumulator.
pub enum AccumulatorEvent<C> {
	Add(C),
	Remove(C),
}

/// How an engine treats an event whose window coordinate is below the per-group
/// high-water mark (an event for an already-closed window).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LatePolicy {
	/// Event-time semantics: drop late events (deterministic under replay).
	#[default]
	Drop,
	/// Accept late events into their (re-opened) window. High-water still
	/// tracks the max coordinate seen but never rejects.
	Process,
}

/// How a finalized window value should be emitted downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitKind {
	Insert,
	Update,
	Remove,
}

/// A finalized window the engine produced; the face turns it into a diff.
pub struct WindowResult<G, Coord, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub span: WindowSpan<Coord>,
	pub value: Output,
	/// The finalized value before this batch's events, when the window was
	/// non-empty (used by faces that emit a real pre on Update/Remove). `None`
	/// for a brand-new window. Faces that don't need it (the sdk drivers)
	/// ignore it.
	pub prior: Option<Output>,
	pub kind: EmitKind,
}

/// Per-group metadata: the highest window start seen, used to drop late events
/// for already-closed windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "K: Serialize", deserialize = "K: serde::de::DeserializeOwned"))]
pub struct GroupMeta<K> {
	pub high_water: Option<K>,
}

impl<K> Default for GroupMeta<K> {
	fn default() -> Self {
		Self {
			high_water: None,
		}
	}
}

/// State-cache key for a group's [`GroupMeta`], tagged so it lives in a
/// distinct keyspace from the per-window accumulators.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct MetaKey(pub EncodedKey);

impl IntoEncodedKey for &MetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		let inner = self.0.as_ref();
		let mut bytes = Vec::with_capacity(1 + inner.len());
		bytes.push(FlowNodeInternalStateKey::WINDOW_META_TAG);
		bytes.extend_from_slice(inner);
		EncodedKey::new(bytes)
	}
}

pub fn meta_key_for<G>(group: &G) -> MetaKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	MetaKey(group.into_encoded_key())
}

pub fn expiry_key<G>(expiry: u64, group: &G, suffix: &[u8]) -> EncodedKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	let group = group.into_encoded_key();
	let group = group.as_ref();
	let mut bytes = Vec::with_capacity(1 + 8 + group.len() + suffix.len());
	bytes.push(FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG);
	bytes.extend_from_slice(&encode_u64(expiry));
	bytes.extend_from_slice(group);
	bytes.extend_from_slice(suffix);
	EncodedKey::new(bytes)
}

pub fn expiry_due_range(threshold: u64) -> EncodedKeyRange {
	let mut start = Vec::with_capacity(1 + 8);
	start.push(FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG);
	start.extend_from_slice(&encode_u64(threshold));
	let end = vec![FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG + 1];
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), Bound::Excluded(EncodedKey::new(end)))
}

#[cfg(test)]
pub(crate) mod test_support {
	use std::{collections::HashMap, ops::Bound};

	use postcard::{from_bytes, to_allocvec};
	use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
	use reifydb_value::{Result, value::row_number::RowNumber};
	use serde::{Deserialize, Serialize, de::DeserializeOwned};

	use crate::{
		key::flow_node_internal_state::FlowNodeInternalStateKey,
		window::{accumulator::WindowAccumulator, store::WindowStore},
	};

	#[derive(Default)]
	pub(crate) struct MockStore {
		data: HashMap<Vec<u8>, Vec<u8>>,
		internal: HashMap<Vec<u8>, Vec<u8>>,
		rows: HashMap<Vec<u8>, u64>,
		next_row: u64,
	}

	impl MockStore {
		pub(crate) fn index_entry_count(&mut self) -> usize {
			self.internal
				.keys()
				.filter(|k| k.first() == Some(&FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG))
				.count()
		}
	}

	impl WindowStore for MockStore {
		fn state_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
			Ok(self.data.get(key.as_bytes()).map(|b| from_bytes(b).expect("decode")))
		}
		fn state_get_many_visit<V: DeserializeOwned>(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_bytes()) {
					visit(key.clone(), from_bytes(b).expect("decode"))?;
				}
			}
			Ok(())
		}
		fn state_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
			self.data.insert(key.as_bytes().to_vec(), to_allocvec(value).expect("encode"));
			Ok(())
		}
		fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.data.remove(key.as_bytes());
			Ok(())
		}
		fn state_drop(&mut self, key: &EncodedKey) -> Result<()> {
			self.data.remove(key.as_bytes());
			Ok(())
		}
		fn internal_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
			Ok(self.internal.get(key.as_bytes()).map(|b| from_bytes(b).expect("decode")))
		}
		fn internal_get_many_visit<V: DeserializeOwned>(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.internal.get(key.as_bytes()) {
					visit(key.clone(), from_bytes(b).expect("decode"))?;
				}
			}
			Ok(())
		}
		fn internal_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
			self.internal.insert(key.as_bytes().to_vec(), to_allocvec(value).expect("encode"));
			Ok(())
		}
		fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.internal.remove(key.as_bytes());
			Ok(())
		}
		fn internal_drop(&mut self, key: &EncodedKey) -> Result<()> {
			self.internal.remove(key.as_bytes());
			Ok(())
		}
		fn internal_range_visit<V: DeserializeOwned>(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
		) -> Result<()> {
			let after_start = |k: &[u8]| match &range.start {
				Bound::Included(s) => k >= s.as_bytes(),
				Bound::Excluded(s) => k > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let before_end = |k: &[u8]| match &range.end {
				Bound::Included(e) => k <= e.as_bytes(),
				Bound::Excluded(e) => k < e.as_bytes(),
				Bound::Unbounded => true,
			};
			let mut matched: Vec<(Vec<u8>, Vec<u8>)> = self
				.internal
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			for (k, b) in matched {
				visit(EncodedKey::new(k), from_bytes(&b).expect("decode"))?;
			}
			Ok(())
		}
		fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
			if let Some(rn) = self.rows.get(key.as_bytes()) {
				return Ok((RowNumber(*rn), false));
			}
			self.next_row += 1;
			self.rows.insert(key.as_bytes().to_vec(), self.next_row);
			Ok((RowNumber(self.next_row), true))
		}
		fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
			keys.iter().map(|k| self.get_or_create_row_number(k)).collect()
		}
		fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
			let start = self.next_row + 1;
			self.next_row += count;
			Ok(RowNumber(start))
		}
		fn clock_now_nanos(&self) -> u64 {
			0
		}
	}

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	pub(crate) struct SumAccumulator {
		pub sum: i64,
		pub count: u64,
	}

	impl WindowAccumulator for SumAccumulator {
		type Contribution = i64;
		type Output = i64;

		fn add(&mut self, contribution: &i64) {
			self.sum += *contribution;
			self.count += 1;
		}
		fn remove(&mut self, contribution: &i64) {
			self.sum -= *contribution;
			self.count = self.count.saturating_sub(1);
		}
		fn finalize(&self) -> Option<i64> {
			if self.count == 0 {
				None
			} else {
				Some(self.sum)
			}
		}
		fn is_empty(&self) -> bool {
			self.count == 0
		}
	}

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	pub(crate) struct StampedSum {
		pub sum: i64,
		pub count: u64,
		pub stamp: Option<u64>,
	}

	impl WindowAccumulator for StampedSum {
		type Contribution = (i64, u64);
		type Output = i64;

		fn add(&mut self, contribution: &(i64, u64)) {
			self.sum += contribution.0;
			self.count += 1;
			self.stamp = Some(self.stamp.map_or(contribution.1, |s| s.max(contribution.1)));
		}
		fn remove(&mut self, contribution: &(i64, u64)) {
			self.sum -= contribution.0;
			self.count = self.count.saturating_sub(1);
		}
		fn finalize(&self) -> Option<i64> {
			if self.count == 0 {
				None
			} else {
				Some(self.sum)
			}
		}
		fn is_empty(&self) -> bool {
			self.count == 0
		}
		fn stamp(&self) -> Option<u64> {
			self.stamp
		}
	}
}
