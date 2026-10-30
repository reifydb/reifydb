// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, mem};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

pub trait Summary<K, V> {
	fn push(&mut self, key: &K, value: &V);

	fn append(&mut self, newer: &Self);

	fn is_empty(&self) -> bool;
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealedAccumulator<K: Ord, V, S> {
	open: BTreeMap<K, V>,
	sealed: S,
}

impl<K: Ord, V, S> SealedAccumulator<K, V, S> {
	pub fn new(sealed: S) -> Self {
		Self {
			open: BTreeMap::new(),
			sealed,
		}
	}
}

impl<K: Ord, V, S: Default> Default for SealedAccumulator<K, V, S> {
	fn default() -> Self {
		Self::new(S::default())
	}
}

impl<K: Ord, V, S: Summary<K, V>> SealedAccumulator<K, V, S> {
	fn seal_open(&mut self) {
		for (key, value) in mem::take(&mut self.open) {
			self.sealed.push(&key, &value);
		}
	}
}

impl<K, V, S> WindowAccumulator for SealedAccumulator<K, V, S>
where
	K: Ord + Clone + Debug,
	V: Clone + Debug + PartialEq,
	S: Summary<K, V> + Clone + Debug + PartialEq + Default,
	SealedAccumulator<K, V, S>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (K, V);
	type Output = S;

	fn add(&mut self, contribution: &(K, V)) {
		self.open.insert(contribution.0.clone(), contribution.1.clone());
	}

	fn remove(&mut self, contribution: &(K, V)) {
		if self.open.get(&contribution.0) == Some(&contribution.1) {
			self.open.remove(&contribution.0);
		}
	}

	fn finalize(&self) -> Option<S> {
		if WindowAccumulator::is_empty(self) {
			return None;
		}
		let mut summary = self.sealed.clone();
		for (key, value) in &self.open {
			summary.push(key, value);
		}
		Some(summary)
	}

	fn is_empty(&self) -> bool {
		self.open.is_empty() && self.sealed.is_empty()
	}
}

impl<K, V, S> MergeAccumulator for SealedAccumulator<K, V, S>
where
	K: Ord + Clone + Debug,
	V: Clone + Debug + PartialEq,
	S: Summary<K, V> + Clone + Debug + PartialEq + Default,
	SealedAccumulator<K, V, S>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		self.seal_open();
		self.sealed.append(&other.sealed);
		for (key, value) in &other.open {
			self.sealed.push(key, value);
		}
	}

	fn compact(&mut self) {
		self.seal_open();
	}
}

impl<K: Ord + HeapSize, V: HeapSize, S: HeapSize> HeapSize for SealedAccumulator<K, V, S> {
	fn heap_size(&self) -> usize {
		self.open.heap_size() + self.sealed.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::decode;

	use super::*;

	#[operator_state]
	#[derive(Debug, Clone, Default, PartialEq)]
	struct Trace {
		pushed: Vec<(u64, i64)>,
	}

	impl Summary<u64, i64> for Trace {
		fn push(&mut self, key: &u64, value: &i64) {
			self.pushed.push((*key, *value));
		}

		fn append(&mut self, newer: &Self) {
			self.pushed.extend_from_slice(&newer.pushed);
		}

		fn is_empty(&self) -> bool {
			self.pushed.is_empty()
		}
	}

	impl HeapSize for Trace {
		fn heap_size(&self) -> usize {
			0
		}
	}

	type Acc = SealedAccumulator<u64, i64, Trace>;

	fn bars(n: u64) -> Vec<(u64, i64)> {
		(0..n).map(|k| (k, k as i64 * 7 - 3)).collect()
	}

	fn pane_of(bars: &[(u64, i64)], compact: bool) -> Acc {
		let mut pane = Acc::default();
		for bar in bars {
			pane.add(bar);
		}
		if compact {
			pane.compact();
		}
		pane
	}

	#[test]
	fn every_pane_split_and_compaction_pushes_each_bar_once_in_key_order() {
		// Brute force: a summary that misses, repeats or reorders a bar breaks every order-sensitive score.
		for n in 0..=6u64 {
			let all = bars(n);
			let cuts = n.saturating_sub(1);
			for split in 0..(1u32 << cuts) {
				let mut panes: Vec<Vec<(u64, i64)>> = vec![Vec::new()];
				for (i, bar) in all.iter().enumerate() {
					if i > 0 && split & (1 << (i - 1)) != 0 {
						panes.push(Vec::new());
					}
					panes.last_mut().unwrap().push(*bar);
				}
				for compacted in 0..(1u32 << panes.len()) {
					let mut merged: Option<Acc> = None;
					for (p, pane) in panes.iter().enumerate() {
						let pane = pane_of(pane, compacted & (1 << p) != 0);
						merged = Some(match merged {
							Some(mut older) => {
								older.merge(&pane);
								older
							}
							None => pane,
						});
					}
					let expected = (n > 0).then(|| Trace {
						pushed: all.clone(),
					});
					assert_eq!(
						merged.and_then(|m| m.finalize()),
						expected,
						"n={n} split={split:b} compacted={compacted:b}"
					);
				}
			}
		}
	}

	#[test]
	fn a_compacted_pane_is_not_empty() {
		// The engine drops empty pane rows, so a compacted pane reported empty would lose its bars.
		let pane = pane_of(&bars(3), true);
		assert!(!pane.is_empty());
		assert_eq!(pane.finalize().map(|t| t.pushed.len()), Some(3));
	}

	#[test]
	fn a_compacted_pane_holds_no_raw_bars() {
		// Raw bars left beside the summary make every stored pane and partial grow with the window.
		let pane = pane_of(&bars(4), true);
		assert!(pane.open.is_empty(), "compact left {} raw bars", pane.open.len());
	}

	#[test]
	fn an_empty_accumulator_finalizes_to_none() {
		let mut accumulator = Acc::default();
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
		accumulator.compact();
		assert!(accumulator.is_empty());
	}

	#[test]
	fn an_open_bar_written_twice_keeps_the_last_value() {
		// An upstream bar re-emitted as a plain add must replace its value, never count twice.
		let mut accumulator = Acc::default();
		accumulator.add(&(1, 10));
		accumulator.add(&(1, 99));
		assert_eq!(accumulator.finalize().map(|t| t.pushed), Some(vec![(1, 99)]));
	}

	#[test]
	fn removing_a_superseded_value_keeps_the_current_one() {
		// An update fans out as remove(pre) then add(post); a reordered pair must not delete the live value.
		let mut accumulator = Acc::default();
		accumulator.add(&(1, 10));
		accumulator.add(&(1, 99));
		accumulator.remove(&(1, 10));
		assert_eq!(accumulator.finalize().map(|t| t.pushed), Some(vec![(1, 99)]));
		accumulator.remove(&(1, 99));
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn a_compacted_accumulator_roundtrips() {
		let mut accumulator = pane_of(&bars(2), true);
		accumulator.add(&(5, 50));
		let bytes = accumulator.encode_state().expect("encode");
		let restored: Acc = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}
}
