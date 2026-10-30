// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions};

use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

const STEPS_PER_OPERATION: usize = 2;

pub trait DabaPanes<A> {
	fn value(&mut self, pane: u64) -> Result<A>;

	fn partial(&mut self, pane: u64) -> Result<A>;

	fn set_partial(&mut self, pane: u64, partial: A) -> Result<()>;

	fn last_below(&mut self, from: u64, below: u64) -> Result<Option<u64>>;
}

#[operator_state]
#[derive(Clone, Debug, PartialEq)]
pub struct DabaQueue<A> {
	front_len: u64,
	back_len: u64,
	l: u64,
	r: u64,
	a: u64,
	b: u64,
	end: u64,
	mid: Option<A>,
	back: Option<A>,
}

impl<A> Default for DabaQueue<A> {
	fn default() -> Self {
		Self {
			front_len: 0,
			back_len: 0,
			l: 0,
			r: 0,
			a: 0,
			b: 0,
			end: 0,
			mid: None,
			back: None,
		}
	}
}

impl<A: HeapSize> HeapSize for DabaQueue<A> {
	fn heap_size(&self) -> usize {
		self.mid.heap_size() + self.back.heap_size()
	}
}

pub struct Merge<A> {
	pub merge: fn(&mut A, &A),
	pub compact: fn(&mut A),
}

impl<A> Clone for Merge<A> {
	fn clone(&self) -> Self {
		*self
	}
}

impl<A> Copy for Merge<A> {}

impl<A: MergeAccumulator> Merge<A> {
	pub fn of() -> Self {
		Self {
			merge: <A as MergeAccumulator>::merge,
			compact: <A as MergeAccumulator>::compact,
		}
	}
}

impl<A: WindowAccumulator> Merge<A> {
	pub fn merged(&self, older: Option<A>, newer: &A) -> A {
		match older {
			Some(mut older) if !older.is_empty() => {
				(self.merge)(&mut older, newer);
				older
			}
			_ => newer.clone(),
		}
	}
}

impl<A: WindowAccumulator> DabaQueue<A> {
	pub fn len(&self) -> u64 {
		self.front_len + self.back_len
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn end(&self) -> u64 {
		self.end
	}

	pub fn push(&mut self, merge: Merge<A>, panes: &mut dyn DabaPanes<A>, pane: u64, value: &A) -> Result<()> {
		reifydb_assertions! {
			assert!(
				self.is_empty() || pane >= self.end,
				"a pane pushed at {pane} sits below the queue end {}; the queue would fold it out of order",
				self.end
			);
		}
		self.back = Some(merge.merged(self.back.take(), value));
		self.back_len += 1;
		self.end = pane + 1;
		self.fixup(merge, panes)
	}

	pub fn pop(&mut self, merge: Merge<A>, panes: &mut dyn DabaPanes<A>) -> Result<()> {
		reifydb_assertions! {
			assert!(self.front_len > 0, "a pop from a queue whose front is empty");
		}
		self.front_len -= 1;
		self.fixup(merge, panes)
	}

	pub fn query(&self, merge: Merge<A>, panes: &mut dyn DabaPanes<A>, front: Option<u64>) -> Result<Option<A>> {
		if self.front_len == 0 {
			return Ok(self.back.clone());
		}
		let Some(front) = front else {
			return Ok(self.back.clone());
		};
		reifydb_assertions! {
			assert!(
				self.mid.is_none() || front < self.r || front >= self.a,
				"the queue front {front} sits in the mid range [{}, {}) that is not converted yet",
				self.r,
				self.a
			);
		}
		let mut total = panes.partial(front)?;
		if front < self.l
			&& let Some(mid) = &self.mid
		{
			(merge.merge)(&mut total, mid);
		}
		Ok(Some(match &self.back {
			Some(back) => merge.merged(Some(total), back),
			None => total,
		}))
	}

	fn fixup(&mut self, merge: Merge<A>, panes: &mut dyn DabaPanes<A>) -> Result<()> {
		if self.mid.is_none() && self.back_len > self.front_len {
			self.mid = self.back.take();
			self.l = self.b;
			self.r = self.b;
			self.a = self.end;
			self.b = self.end;
			self.front_len += self.back_len;
			self.back_len = 0;
		}
		for _ in 0..STEPS_PER_OPERATION {
			self.step(merge, panes)?;
		}
		Ok(())
	}

	fn step(&mut self, merge: Merge<A>, panes: &mut dyn DabaPanes<A>) -> Result<()> {
		let Some(mid) = &self.mid else {
			return Ok(());
		};
		if self.a > self.r {
			match panes.last_below(self.r, self.a)? {
				None => self.a = self.r,
				Some(pane) => {
					let value = panes.value(pane)?;
					let partial = if self.a < self.b {
						merge.merged(Some(value), &panes.partial(self.a)?)
					} else {
						value
					};
					panes.set_partial(pane, partial)?;
					self.a = pane;
				}
			}
			return Ok(());
		}
		match panes.last_below(0, self.l)? {
			None => {
				self.l = self.b;
				self.r = self.b;
				self.a = self.b;
				self.mid = None;
			}
			Some(pane) => {
				let partial = merge.merged(Some(panes.partial(pane)?), mid);
				panes.set_partial(pane, partial)?;
				self.l = pane;
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::metrics::heap::HeapSize;
	use reifydb_macro::operator_state;
	use reifydb_value::Result;

	use super::{DabaPanes, DabaQueue, Merge};
	use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

	#[operator_state]
	#[derive(Clone, Debug, Default, PartialEq)]
	struct Trail {
		panes: Vec<u64>,
		max: Option<i64>,
		min: Option<i64>,
		first: Option<i64>,
		last: Option<i64>,
		sum: i64,
	}

	impl HeapSize for Trail {
		fn heap_size(&self) -> usize {
			self.panes.capacity() * 8
		}
	}

	impl Trail {
		fn of(pane: u64, value: i64) -> Self {
			Self {
				panes: vec![pane],
				max: Some(value),
				min: Some(value),
				first: Some(value),
				last: Some(value),
				sum: value,
			}
		}
	}

	impl WindowAccumulator for Trail {
		type Contribution = (u64, i64);
		type Output = Trail;

		fn add(&mut self, contribution: &(u64, i64)) {
			self.merge(&Trail::of(contribution.0, contribution.1));
		}

		fn remove(&mut self, _contribution: &(u64, i64)) {
			unreachable!("the queue never retracts")
		}

		fn finalize(&self) -> Option<Trail> {
			(!self.is_empty()).then(|| self.clone())
		}

		fn is_empty(&self) -> bool {
			self.panes.is_empty()
		}
	}

	impl MergeAccumulator for Trail {
		fn merge(&mut self, other: &Self) {
			if other.is_empty() {
				return;
			}
			if self.is_empty() {
				*self = other.clone();
				return;
			}
			self.panes.extend_from_slice(&other.panes);
			self.max = self.max.max(other.max);
			self.min = self.min.min(other.min);
			self.last = other.last;
			self.sum += other.sum;
		}
	}

	#[derive(Default)]
	struct Panes {
		rows: BTreeMap<u64, (Trail, Option<Trail>)>,
		lookups: usize,
		reads: usize,
	}

	impl DabaPanes<Trail> for Panes {
		fn value(&mut self, pane: u64) -> Result<Trail> {
			self.reads += 1;
			Ok(self.rows.get(&pane).expect("value of a missing pane").0.clone())
		}

		fn partial(&mut self, pane: u64) -> Result<Trail> {
			self.reads += 1;
			Ok(self.rows
				.get(&pane)
				.and_then(|row| row.1.clone())
				.expect("partial of a pane never converted"))
		}

		fn set_partial(&mut self, pane: u64, partial: Trail) -> Result<()> {
			self.rows.get_mut(&pane).expect("partial set on a missing pane").1 = Some(partial);
			Ok(())
		}

		fn last_below(&mut self, from: u64, below: u64) -> Result<Option<u64>> {
			self.lookups += 1;
			Ok(self.rows.range(from..below).next_back().map(|(pane, _)| *pane))
		}
	}

	struct Lcg(u64);

	impl Lcg {
		fn below(&mut self, bound: u64) -> u64 {
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(self.0 >> 33) % bound
		}
	}

	fn oracle(window: &BTreeMap<u64, i64>) -> Option<Trail> {
		let mut total: Option<Trail> = None;
		for (pane, value) in window {
			let one = Trail::of(*pane, *value);
			total = Some(match total {
				Some(mut t) => {
					t.merge(&one);
					t
				}
				None => one,
			});
		}
		total
	}

	fn run(seed: u64, operations: usize, push_percent: u64) {
		let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
		let mut queue: DabaQueue<Trail> = DabaQueue::default();
		let mut panes = Panes::default();
		let mut window: BTreeMap<u64, i64> = BTreeMap::new();
		let mut next = 0u64;
		for op in 0..operations {
			let before = (panes.lookups, panes.reads);
			if window.is_empty() || rng.below(100) < push_percent {
				next += 1 + rng.below(3);
				let value = rng.below(100) as i64 - 50;
				let one = Trail::of(next, value);
				panes.rows.insert(next, (one.clone(), None));
				window.insert(next, value);
				queue.push(Merge::of(), &mut panes, next, &one).unwrap();
			} else {
				let (front, _) = window.pop_first().unwrap();
				panes.rows.remove(&front);
				queue.pop(Merge::of(), &mut panes).unwrap();
			}
			// Worst case O(1): a fix-up that walked the queue would read far more than this per operation.
			assert!(
				panes.lookups - before.0 <= 2 && panes.reads - before.1 <= 4,
				"seed {seed} op {op}: one operation did {} lookups and {} reads",
				panes.lookups - before.0,
				panes.reads - before.1
			);
			assert_eq!(queue.len(), window.len() as u64, "seed {seed} op {op}: queue length drifted");
			let front = window.keys().next().copied();
			assert_eq!(
				queue.query(Merge::of(), &mut panes, front).unwrap(),
				oracle(&window),
				"seed {seed} op {op}: queue aggregate differs from folding the window oldest first"
			);
		}
	}

	#[test]
	fn the_queue_matches_a_brute_force_fold_under_mixed_pushes_and_pops() {
		// Trail records pane order, so any fix-up that merges out of order or skips a pane changes the answer.
		for seed in 0..200 {
			run(seed, 400, 55);
		}
	}

	#[test]
	fn the_queue_matches_a_brute_force_fold_when_pops_dominate() {
		// Draining fast exhausts the front while a rebuild is running, the case a lazy flip gets wrong.
		for seed in 0..200 {
			run(1_000 + seed, 400, 35);
		}
	}

	#[test]
	fn a_growing_queue_keeps_bounded_work_per_push() {
		// A pure push run would trigger one large flip in an amortized queue; here every push stays O(1).
		run(7, 5_000, 100);
	}

	#[test]
	fn an_emptied_queue_answers_none_and_accepts_new_pushes() {
		// Without the reset to idle a drained queue would keep a stale mid and fold it into the next window.
		let mut queue: DabaQueue<Trail> = DabaQueue::default();
		let mut panes = Panes::default();
		for pane in [1u64, 2, 3] {
			let one = Trail::of(pane, pane as i64);
			panes.rows.insert(pane, (one.clone(), None));
			queue.push(Merge::of(), &mut panes, pane, &one).unwrap();
		}
		for pane in [1u64, 2, 3] {
			panes.rows.remove(&pane);
			queue.pop(Merge::of(), &mut panes).unwrap();
		}
		assert!(queue.is_empty());
		assert_eq!(queue.query(Merge::of(), &mut panes, None).unwrap(), None);
		let one = Trail::of(9, 4);
		panes.rows.insert(9, (one.clone(), None));
		queue.push(Merge::of(), &mut panes, 9, &one).unwrap();
		assert_eq!(queue.query(Merge::of(), &mut panes, Some(9)).unwrap(), Some(Trail::of(9, 4)));
	}
}
