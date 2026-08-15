// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	fmt::{self, Debug, Formatter},
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use super::WindowAccumulator;
use crate::{
	operator::state::seal::coord::Coord,
	window::span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
struct SealingBase<C: Slot, V> {
	amendable: Option<SlotSpan<C>>,
	high_water: Option<C>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V> Default for SealingBase<C, V> {
	fn default() -> Self {
		Self {
			amendable: None,
			high_water: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V> SealingBase<C, V> {
	fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			amendable: Some(amendable),
			high_water: None,
			tail: BTreeMap::new(),
		}
	}

	fn push(&mut self, coord: C, value: V) -> Vec<(C, V)> {
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, value);
		let mut aged = Vec::new();
		let (Some(hw), Some(l)) = (self.high_water, self.amendable) else {
			return aged;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw.order_key().span_since(c.order_key()) > l {
				aged.push(self.tail.pop_first().expect("non-empty"));
			} else {
				break;
			}
		}
		aged
	}

	fn remove(&mut self, coord: &C) {
		self.tail.remove(coord);
	}

	fn tail(&self) -> &BTreeMap<C, V> {
		&self.tail
	}

	fn is_tail_empty(&self) -> bool {
		self.tail.is_empty()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingMax<C: Slot, V: Ord> {
	base: SealingBase<C, V>,
	sealed: Option<V>,
}

impl<C: Slot, V: Ord> Default for SealingMax<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: None,
		}
	}
}

impl<C: Slot, V: Ord + Clone> SealingMax<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed: None,
		}
	}

	pub fn max(&self) -> Option<V> {
		let tail_max = self.base.tail().values().max().cloned();
		match (self.sealed.clone(), tail_max) {
			(Some(s), Some(t)) => Some(s.max(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some(s) = other.sealed.clone() {
			self.seal(s);
		}
		for (coord, value) in other.base.tail() {
			for (_, aged) in self.base.push(*coord, value.clone()) {
				self.seal(aged);
			}
		}
	}

	fn seal(&mut self, v: V) {
		self.sealed = Some(match self.sealed.take() {
			Some(s) => s.max(v),
			None => v,
		});
	}
}

impl<C, V> WindowAccumulator for SealingMax<C, V>
where
	C: Slot + Hash,
	V: Ord + Clone + Debug,
	SealingMax<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = V;

	fn add(&mut self, contribution: &(C, V)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.sealed = Some(match self.sealed.take() {
				Some(s) => s.max(v),
				None => v,
			});
		}
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.max()
	}

	fn is_empty(&self) -> bool {
		self.sealed.is_none() && self.base.is_tail_empty()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingMin<C: Slot, V: Ord> {
	base: SealingBase<C, V>,
	sealed: Option<V>,
}

impl<C: Slot, V: Ord> Default for SealingMin<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: None,
		}
	}
}

impl<C: Slot, V: Ord + Clone> SealingMin<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed: None,
		}
	}

	pub fn min(&self) -> Option<V> {
		let tail_min = self.base.tail().values().min().cloned();
		match (self.sealed.clone(), tail_min) {
			(Some(s), Some(t)) => Some(s.min(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some(s) = other.sealed.clone() {
			self.seal(s);
		}
		for (coord, value) in other.base.tail() {
			for (_, aged) in self.base.push(*coord, value.clone()) {
				self.seal(aged);
			}
		}
	}

	fn seal(&mut self, v: V) {
		self.sealed = Some(match self.sealed.take() {
			Some(s) => s.min(v),
			None => v,
		});
	}
}

impl<C, V> WindowAccumulator for SealingMin<C, V>
where
	C: Slot + Hash,
	V: Ord + Clone + Debug,
	SealingMin<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = V;

	fn add(&mut self, contribution: &(C, V)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.sealed = Some(match self.sealed.take() {
				Some(s) => s.min(v),
				None => v,
			});
		}
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.min()
	}

	fn is_empty(&self) -> bool {
		self.sealed.is_none() && self.base.is_tail_empty()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingEndpoint<C: Slot, V> {
	base: SealingBase<C, V>,
	sealed_open: Option<(C, V)>,
}

impl<C: Slot, V> Default for SealingEndpoint<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed_open: None,
		}
	}
}

impl<C: Slot, V: Clone> SealingEndpoint<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed_open: None,
		}
	}

	pub fn open(&self) -> Option<&V> {
		match &self.sealed_open {
			Some((_, v)) => Some(v),
			None => self.base.tail().values().next(),
		}
	}

	pub fn close(&self) -> Option<&V> {
		match self.base.tail().values().next_back() {
			Some(v) => Some(v),
			None => self.sealed_open.as_ref().map(|(_, v)| v),
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some((c, v)) = other.sealed_open.clone() {
			self.seal_open(c, v);
		}
		for (coord, value) in other.base.tail() {
			for (c, v) in self.base.push(*coord, value.clone()) {
				self.seal_open(c, v);
			}
		}
	}

	fn seal_open(&mut self, c: C, v: V) {
		self.sealed_open = Some(match self.sealed_open.take() {
			Some((sc, sv)) if sc <= c => (sc, sv),
			_ => (c, v),
		});
	}
}

impl<C, V> WindowAccumulator for SealingEndpoint<C, V>
where
	C: Slot + Hash,
	V: Clone + Debug + PartialEq,
	SealingEndpoint<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = (V, V);

	fn add(&mut self, contribution: &(C, V)) {
		for (c, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.sealed_open = Some(match self.sealed_open.take() {
				Some((sc, sv)) if sc <= c => (sc, sv),
				_ => (c, v),
			});
		}
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<(V, V)> {
		match (self.open(), self.close()) {
			(Some(o), Some(c)) => Some((o.clone(), c.clone())),
			_ => None,
		}
	}

	fn is_empty(&self) -> bool {
		self.sealed_open.is_none() && self.base.is_tail_empty()
	}
}

pub trait SealFold {
	type Value: Clone + Debug;
	type State: Clone + Debug + Default;
	type Output: Clone + Debug + PartialEq;

	fn fold(state: &mut Self::State, prev: Option<&Self::Value>, cur: &Self::Value);

	fn output(state: &Self::State) -> Option<Self::Output>;
}

#[operator_state]
pub struct SealingFold<C: Slot, F: SealFold> {
	base: SealingBase<C, F::Value>,
	sealed: F::State,
	last_sealed: Option<F::Value>,
	marker: PhantomData<fn() -> F>,
}

impl<C: Slot, F: SealFold> Clone for SealingFold<C, F> {
	fn clone(&self) -> Self {
		Self {
			base: self.base.clone(),
			sealed: self.sealed.clone(),
			last_sealed: self.last_sealed.clone(),
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> Debug for SealingFold<C, F> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("SealingFold")
			.field("base", &self.base)
			.field("sealed", &self.sealed)
			.field("last_sealed", &self.last_sealed)
			.finish()
	}
}

impl<C: Slot, F: SealFold> Default for SealingFold<C, F> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> SealingFold<C, F> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}
}

impl<C, F> WindowAccumulator for SealingFold<C, F>
where
	C: Slot + Hash,
	F: SealFold,
	SealingFold<C, F>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, F::Value);
	type Output = F::Output;

	fn add(&mut self, contribution: &(C, F::Value)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			F::fold(&mut self.sealed, self.last_sealed.as_ref(), &v);
			self.last_sealed = Some(v);
		}
	}

	fn remove(&mut self, contribution: &(C, F::Value)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<F::Output> {
		let mut state = self.sealed.clone();
		let mut prev = self.last_sealed.clone();
		for v in self.base.tail().values() {
			F::fold(&mut state, prev.as_ref(), v);
			prev = Some(v.clone());
		}
		F::output(&state)
	}

	fn is_empty(&self) -> bool {
		self.last_sealed.is_none() && self.base.is_tail_empty()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingTail<C: Slot, V> {
	base: SealingBase<C, V>,
}

impl<C: Slot, V> Default for SealingTail<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
		}
	}
}

impl<C: Slot, V: Clone> SealingTail<C, V> {
	pub fn add(&mut self, coord: C, value: V) {
		self.base.push(coord, value);
	}

	pub fn remove(&mut self, coord: &C) {
		self.base.remove(coord);
	}

	pub fn tail(&self) -> &BTreeMap<C, V> {
		self.base.tail()
	}

	pub fn is_empty(&self) -> bool {
		self.base.is_tail_empty()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct TailAccumulator<C: Slot, V> {
	events: SealingTail<C, V>,
}

impl<C: Slot, V> Default for TailAccumulator<C, V> {
	fn default() -> Self {
		Self {
			events: SealingTail::default(),
		}
	}
}


impl<C, V> WindowAccumulator for TailAccumulator<C, V>
where
	C: Slot,
	V: Clone + Debug + PartialEq,
	TailAccumulator<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = BTreeMap<C, V>;

	fn add(&mut self, contribution: &(C, V)) {
		self.events.add(contribution.0, contribution.1.clone());
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.events.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<BTreeMap<C, V>> {
		(!self.events.is_empty()).then(|| self.events.tail().clone())
	}

	fn is_empty(&self) -> bool {
		self.events.is_empty()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for SealingBase<C, V> {
	fn heap_size(&self) -> usize {
		self.high_water.heap_size() + self.tail.heap_size()
	}
}

impl<C: Slot + HeapSize, V: Ord + HeapSize> HeapSize for SealingMax<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size()
	}
}

impl<C: Slot + HeapSize, V: Ord + HeapSize> HeapSize for SealingMin<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for SealingEndpoint<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed_open.heap_size()
	}
}

impl<C: Slot + HeapSize, F: SealFold> HeapSize for SealingFold<C, F>
where
	F::Value: HeapSize,
	F::State: HeapSize,
{
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size() + self.last_sealed.heap_size()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for SealingTail<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for TailAccumulator<C, V> {
	fn heap_size(&self) -> usize {
		self.events.heap_size()
	}
}
