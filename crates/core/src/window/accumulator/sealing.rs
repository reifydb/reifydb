// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	fmt::{self, Debug, Formatter},
	hash::Hash,
	marker::PhantomData,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::WindowAccumulator;
use crate::window::span::Slot;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
struct SealingBase<C: Slot, V> {
	lateness: Option<C::Duration>,
	high_water: Option<C>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V> Default for SealingBase<C, V> {
	fn default() -> Self {
		Self {
			lateness: None,
			high_water: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V> SealingBase<C, V> {
	fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			lateness: Some(lateness),
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
		let (Some(hw), Some(l)) = (self.high_water, self.lateness) else {
			return aged;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw - c > l {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
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
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			base: SealingBase::with_lateness(lateness),
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
	C: Slot + Hash + Serialize + DeserializeOwned,
	C::Duration: Serialize + DeserializeOwned,
	V: Ord + Clone + Debug + Serialize + DeserializeOwned,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
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
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			base: SealingBase::with_lateness(lateness),
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
	C: Slot + Hash + Serialize + DeserializeOwned,
	C::Duration: Serialize + DeserializeOwned,
	V: Ord + Clone + Debug + Serialize + DeserializeOwned,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
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
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			base: SealingBase::with_lateness(lateness),
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
	C: Slot + Hash + Serialize + DeserializeOwned,
	C::Duration: Serialize + DeserializeOwned,
	V: Clone + Debug + PartialEq + Serialize + DeserializeOwned,
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
	type Value: Clone + Debug + Serialize + DeserializeOwned;
	type State: Clone + Debug + Default + Serialize + DeserializeOwned;
	type Output: Clone + Debug + PartialEq;

	fn fold(state: &mut Self::State, prev: Option<&Self::Value>, cur: &Self::Value);

	fn output(state: &Self::State) -> Option<Self::Output>;
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, F::State: Serialize, F::Value: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, F::State: serde::de::DeserializeOwned, F::Value: serde::de::DeserializeOwned"
))]
pub struct SealingFold<C: Slot, F: SealFold> {
	base: SealingBase<C, F::Value>,
	sealed: F::State,
	last_sealed: Option<F::Value>,
	#[serde(skip)]
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
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			base: SealingBase::with_lateness(lateness),
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}
}

impl<C, F> WindowAccumulator for SealingFold<C, F>
where
	C: Slot + Hash + Serialize + DeserializeOwned,
	C::Duration: Serialize + DeserializeOwned,
	F: SealFold,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
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
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			base: SealingBase::with_lateness(lateness),
		}
	}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
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

impl<C: Slot, V: Clone> TailAccumulator<C, V> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			events: SealingTail::with_lateness(lateness),
		}
	}
}

impl<C, V> WindowAccumulator for TailAccumulator<C, V>
where
	C: Slot + Serialize + DeserializeOwned,
	C::Duration: Serialize + DeserializeOwned,
	V: Clone + Debug + PartialEq + Serialize + DeserializeOwned,
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
