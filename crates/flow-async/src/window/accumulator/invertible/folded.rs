// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, mem};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

pub trait Fold<T> {
	fn fold(&mut self, value: &T);

	fn append(&mut self, newer: &Self);

	fn is_empty(&self) -> bool;
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct FoldedAccumulator<A, S> {
	open: A,
	sealed: S,
}

impl<A: Default, S> FoldedAccumulator<A, S> {
	pub fn new(sealed: S) -> Self {
		Self {
			open: A::default(),
			sealed,
		}
	}
}

impl<A: Default, S: Default> Default for FoldedAccumulator<A, S> {
	fn default() -> Self {
		Self::new(S::default())
	}
}

impl<A: WindowAccumulator, S: Fold<A::Output>> FoldedAccumulator<A, S> {
	fn seal_open(&mut self) {
		if let Some(value) = mem::take(&mut self.open).finalize() {
			self.sealed.fold(&value);
		}
	}
}

impl<A, S> WindowAccumulator for FoldedAccumulator<A, S>
where
	A: WindowAccumulator,
	S: Fold<A::Output> + Clone + Debug + PartialEq + Default,
	FoldedAccumulator<A, S>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = A::Contribution;
	type Output = S;

	fn add(&mut self, contribution: &A::Contribution) {
		self.open.add(contribution);
	}

	fn remove(&mut self, contribution: &A::Contribution) {
		self.open.remove(contribution);
	}

	fn finalize(&self) -> Option<S> {
		if WindowAccumulator::is_empty(self) {
			return None;
		}
		let mut summary = self.sealed.clone();
		if let Some(value) = self.open.finalize() {
			summary.fold(&value);
		}
		Some(summary)
	}

	fn is_empty(&self) -> bool {
		self.open.is_empty() && self.sealed.is_empty()
	}
}

impl<A, S> MergeAccumulator for FoldedAccumulator<A, S>
where
	A: WindowAccumulator,
	S: Fold<A::Output> + Clone + Debug + PartialEq + Default,
	FoldedAccumulator<A, S>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		self.seal_open();
		self.sealed.append(&other.sealed);
		if let Some(value) = other.open.finalize() {
			self.sealed.fold(&value);
		}
	}

	fn compact(&mut self) {
		self.seal_open();
	}
}

impl<A: HeapSize, S: HeapSize> HeapSize for FoldedAccumulator<A, S> {
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
	struct PaneSum {
		n: u64,
		total: i64,
	}

	impl HeapSize for PaneSum {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for PaneSum {
		type Contribution = i64;
		type Output = (u64, i64);

		fn add(&mut self, contribution: &i64) {
			self.n += 1;
			self.total += contribution;
		}

		fn remove(&mut self, contribution: &i64) {
			self.n -= 1;
			self.total -= contribution;
		}

		fn finalize(&self) -> Option<(u64, i64)> {
			(self.n > 0).then_some((self.n, self.total))
		}

		fn is_empty(&self) -> bool {
			self.n == 0
		}
	}

	#[operator_state]
	#[derive(Debug, Clone, Default, PartialEq)]
	struct Trace {
		panes: Vec<(u64, i64)>,
	}

	impl Fold<(u64, i64)> for Trace {
		fn fold(&mut self, value: &(u64, i64)) {
			self.panes.push(*value);
		}

		fn append(&mut self, newer: &Self) {
			self.panes.extend_from_slice(&newer.panes);
		}

		fn is_empty(&self) -> bool {
			self.panes.is_empty()
		}
	}

	impl HeapSize for Trace {
		fn heap_size(&self) -> usize {
			0
		}
	}

	type Acc = FoldedAccumulator<PaneSum, Trace>;

	fn pane_of(values: &[i64], compact: bool) -> Acc {
		let mut pane = Acc::default();
		for value in values {
			pane.add(value);
		}
		if compact {
			pane.compact();
		}
		pane
	}

	#[test]
	fn every_pane_split_and_compaction_folds_each_pane_once_in_order() {
		// Brute force: a pane folded twice, lost, or folded out of order corrupts every summed or counted
		// summary.
		let panes: [&[i64]; 5] = [&[3, 4], &[10], &[1, 1, 1], &[7], &[-2, 5]];
		let expected: Vec<(u64, i64)> = panes.iter().map(|p| (p.len() as u64, p.iter().sum())).collect();
		for compacted in 0..(1u32 << panes.len()) {
			for first in 0..panes.len() {
				let mut merged = pane_of(panes[first], compacted & (1 << first) != 0);
				let mut prefix = Acc::default();
				for (p, values) in panes.iter().enumerate().take(first) {
					prefix.merge(&pane_of(values, compacted & (1 << p) != 0));
				}
				for (p, values) in panes.iter().enumerate().skip(first + 1) {
					merged.merge(&pane_of(values, compacted & (1 << p) != 0));
				}
				prefix.merge(&merged);
				assert_eq!(
					prefix.finalize().map(|t| t.panes),
					Some(expected.clone()),
					"compacted={compacted:b} first={first}"
				);
			}
		}
	}

	#[test]
	fn a_compacted_pane_is_not_empty_and_holds_no_open_part() {
		// The engine drops empty pane rows, and an open part left beside the summary grows every stored
		// partial.
		let pane = pane_of(&[1, 2], true);
		assert!(!pane.is_empty());
		assert!(pane.open.is_empty());
		assert_eq!(pane.finalize().map(|t| t.panes), Some(vec![(2, 3)]));
	}

	#[test]
	fn a_retraction_in_the_open_pane_is_undone_before_it_folds() {
		// Retraction must reach the open part; a value folded early could never be taken back.
		let mut pane = pane_of(&[5, 6], false);
		pane.remove(&6);
		assert_eq!(pane.finalize().map(|t| t.panes), Some(vec![(1, 5)]));
		pane.remove(&5);
		assert_eq!(pane.finalize(), None);
	}

	#[test]
	fn merging_an_empty_accumulator_folds_nothing() {
		// An empty pane folded as a value would count a pane with no data.
		let mut merged = pane_of(&[4], false);
		merged.merge(&Acc::default());
		assert_eq!(merged.finalize().map(|t| t.panes), Some(vec![(1, 4)]));
		let mut empty = Acc::default();
		empty.compact();
		assert!(empty.is_empty());
	}

	#[test]
	fn a_compacted_accumulator_roundtrips() {
		let mut accumulator = pane_of(&[1, 2], true);
		accumulator.add(&9);
		let bytes = accumulator.encode_state().expect("encode");
		let restored: Acc = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}
}
