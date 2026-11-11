// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, iter::Zip, ops::Range, vec::IntoIter as VecIntoIter};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

pub mod invertible;

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(crate) mod testkit;

pub trait ContributionSlice: Copy {
	fn len(&self) -> usize;

	fn is_empty(&self) -> bool {
		self.len() == 0
	}

	fn range(self, range: Range<usize>) -> Self;
}

impl<T> ContributionSlice for &[T] {
	fn len(&self) -> usize {
		<[T]>::len(self)
	}

	fn range(self, range: Range<usize>) -> Self {
		&self[range]
	}
}

pub trait Contributions: Clone + Debug + Default {
	type Item: Clone + Debug;
	type Slice<'a>: ContributionSlice
	where
		Self: 'a;

	fn push(&mut self, item: Self::Item);

	fn len(&self) -> usize;

	fn is_empty(&self) -> bool {
		self.len() == 0
	}

	fn slice(&self) -> Self::Slice<'_>;
}

impl<T: Clone + Debug> Contributions for Vec<T> {
	type Item = T;
	type Slice<'a>
		= &'a [T]
	where
		T: 'a;

	fn push(&mut self, item: T) {
		Vec::push(self, item);
	}

	fn len(&self) -> usize {
		Vec::len(self)
	}

	fn slice(&self) -> &[T] {
		self
	}
}

pub type Contribution<A> = <<A as WindowAccumulator>::Contributions as Contributions>::Item;

pub type Slice<'a, A> = <<A as WindowAccumulator>::Contributions as Contributions>::Slice<'a>;

pub trait WindowAccumulator: Clone + Debug + Default + OperatorState + StateCodec + HeapSize {
	type Contributions: Contributions;
	type Output: Clone + Debug + PartialEq;

	fn add(&mut self, contributions: Slice<'_, Self>);

	fn remove(&mut self, contributions: Slice<'_, Self>);

	fn finalize(&self) -> Option<Self::Output>;

	fn rows(&self) -> Option<u64> {
		None
	}
}

pub fn one<X: Contributions>(item: X::Item) -> X {
	let mut contributions = X::default();
	contributions.push(item);
	contributions
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccumulatorOp {
	Add,
	Remove,
}

#[derive(Clone, Debug, Default)]
pub struct AccumulatorEvents<X> {
	ops: Vec<AccumulatorOp>,
	contributions: X,
}

impl<X: Contributions> AccumulatorEvents<X> {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn push(&mut self, op: AccumulatorOp, contribution: X::Item) {
		self.ops.push(op);
		self.contributions.push(contribution);
	}

	pub fn add(&mut self, contribution: X::Item) {
		self.push(AccumulatorOp::Add, contribution);
	}

	pub fn remove(&mut self, contribution: X::Item) {
		self.push(AccumulatorOp::Remove, contribution);
	}

	pub fn len(&self) -> usize {
		self.ops.len()
	}

	pub fn is_empty(&self) -> bool {
		self.ops.is_empty()
	}

	pub fn ops(&self) -> &[AccumulatorOp] {
		&self.ops
	}

	pub fn contributions(&self) -> &X {
		&self.contributions
	}

	pub fn runs(&self) -> Runs<'_> {
		Runs {
			ops: &self.ops,
			at: 0,
		}
	}
}

impl<T> IntoIterator for AccumulatorEvents<Vec<T>> {
	type Item = (AccumulatorOp, T);
	type IntoIter = Zip<VecIntoIter<AccumulatorOp>, VecIntoIter<T>>;

	fn into_iter(self) -> Self::IntoIter {
		self.ops.into_iter().zip(self.contributions)
	}
}

pub struct Runs<'a> {
	ops: &'a [AccumulatorOp],
	at: usize,
}

impl Iterator for Runs<'_> {
	type Item = (AccumulatorOp, Range<usize>);

	fn next(&mut self) -> Option<Self::Item> {
		let op = *self.ops.get(self.at)?;
		let start = self.at;
		while self.ops.get(self.at) == Some(&op) {
			self.at += 1;
		}
		Some((op, start..self.at))
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Applied {
	pub touched: bool,
	pub dropped: u64,
}

pub trait MergeAccumulator: WindowAccumulator {
	fn merge(&mut self, other: &Self);

	fn compact(&mut self) {}

	fn compacts(&self) -> bool {
		true
	}
}

pub trait UnmergeAccumulator: MergeAccumulator {
	fn unmerge(&mut self, other: &Self);
}

#[operator_state]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CountedAccumulator<A> {
	acc: A,
	live: u64,
}

impl<A> CountedAccumulator<A> {
	pub fn new(acc: A) -> Self {
		Self {
			acc,
			live: 0,
		}
	}

	pub fn inner(&self) -> &A {
		&self.acc
	}

	pub fn live(&self) -> u64 {
		self.live
	}

	pub fn is_empty(&self) -> bool {
		self.live == 0
	}

	pub fn merge_by(&mut self, other: &Self, merge: fn(&mut A, &A))
	where
		A: WindowAccumulator,
	{
		merge(&mut self.acc, &other.acc);
		self.sync(self.live + other.live);
	}

	pub fn compact_by(&mut self, compact: fn(&mut A)) {
		compact(&mut self.acc);
	}
}

impl<A: WindowAccumulator> CountedAccumulator<A> {
	pub fn apply(&mut self, events: &AccumulatorEvents<A::Contributions>) -> Applied {
		let contributions = events.contributions().slice();
		let mut applied = Applied::default();
		for (op, run) in events.runs() {
			let applied_run = match op {
				AccumulatorOp::Add => self.add_run(contributions.range(run)),
				AccumulatorOp::Remove => self.remove_run(contributions.range(run)),
			};
			applied.touched |= applied_run.touched;
			applied.dropped += applied_run.dropped;
		}
		applied
	}

	fn sync(&mut self, events: u64) {
		self.live = self.acc.rows().unwrap_or(events);
	}

	fn add_run(&mut self, run: Slice<'_, A>) -> Applied {
		if run.is_empty() {
			return Applied::default();
		}
		self.acc.add(run);
		self.sync(self.live + run.len() as u64);
		Applied {
			touched: true,
			dropped: 0,
		}
	}

	fn remove_run(&mut self, run: Slice<'_, A>) -> Applied {
		let cut = run.len().min(usize::try_from(self.live).unwrap_or(usize::MAX));
		if cut > 0 {
			self.acc.remove(run.range(0..cut));
			self.sync(self.live - cut as u64);
		}
		Applied {
			touched: cut > 0,
			dropped: (run.len() - cut) as u64,
		}
	}
}

impl<A: HeapSize> HeapSize for CountedAccumulator<A> {
	fn heap_size(&self) -> usize {
		self.acc.heap_size()
	}
}

impl<A> WindowAccumulator for CountedAccumulator<A>
where
	A: WindowAccumulator,
	CountedAccumulator<A>: OperatorState + StateCodec + HeapSize,
{
	type Contributions = A::Contributions;
	type Output = A::Output;

	fn add(&mut self, contributions: Slice<'_, A>) {
		self.add_run(contributions);
	}

	fn remove(&mut self, contributions: Slice<'_, A>) {
		self.remove_run(contributions);
	}

	fn finalize(&self) -> Option<A::Output> {
		if self.live == 0 {
			return None;
		}
		self.acc.finalize()
	}
}

impl<A> MergeAccumulator for CountedAccumulator<A>
where
	A: MergeAccumulator,
	CountedAccumulator<A>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		self.merge_by(other, A::merge);
	}

	fn compact(&mut self) {
		self.acc.compact();
	}

	fn compacts(&self) -> bool {
		self.acc.compacts()
	}
}

impl<A> UnmergeAccumulator for CountedAccumulator<A>
where
	A: UnmergeAccumulator,
	CountedAccumulator<A>: OperatorState + StateCodec + HeapSize,
{
	fn unmerge(&mut self, other: &Self) {
		self.acc.unmerge(&other.acc);
		let events = self.live.checked_sub(other.live).unwrap_or_else(|| {
			panic!("CountedAccumulator unmerge of {} live rows from {} live rows", other.live, self.live)
		});
		self.sync(events);
	}
}
