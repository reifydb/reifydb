// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::BTreeMap,
	fmt::{self, Debug, Formatter},
	hash::{Hash, Hasher},
	marker::PhantomData,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::operator::windowed::span::Slot;

pub trait WindowAccumulator: Clone + Debug + Default + Serialize + DeserializeOwned {
	type Contribution: Clone + Debug;
	type Output: Clone + Debug + PartialEq;

	fn add(&mut self, contribution: &Self::Contribution);

	fn remove(&mut self, contribution: &Self::Contribution);

	fn finalize(&self) -> Option<Self::Output>;

	fn is_empty(&self) -> bool;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Moments {
	n: u64,
	sum: f64,
	sum_sq: f64,
}

impl WindowAccumulator for Moments {
	type Contribution = f64;
	type Output = Moments;

	fn add(&mut self, contribution: &f64) {
		Moments::add(self, *contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		Moments::remove(self, *contribution);
	}

	fn finalize(&self) -> Option<Moments> {
		(self.n > 0).then_some(*self)
	}

	fn is_empty(&self) -> bool {
		self.n == 0
	}
}

impl Moments {
	#[inline]
	pub fn add(&mut self, x: f64) {
		self.n += 1;
		self.sum += x;
		self.sum_sq += x * x;
	}

	#[inline]
	pub fn remove(&mut self, x: f64) {
		#[cfg(reifydb_assertions)]
		{
			assert!(self.n > 0, "Moments::remove on empty accumulator");
		}
		self.n -= 1;
		if self.n == 0 {
			self.sum = 0.0;
			self.sum_sq = 0.0;
			return;
		}
		self.sum -= x;
		self.sum_sq -= x * x;
	}

	#[inline]
	pub fn count(&self) -> u64 {
		self.n
	}

	#[inline]
	pub fn sum(&self) -> f64 {
		self.sum
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.n == 0
	}

	pub fn mean(&self) -> Option<f64> {
		(self.n > 0).then(|| self.sum / self.n as f64)
	}

	pub fn variance_pop(&self) -> Option<f64> {
		(self.n > 0).then(|| {
			let mean = self.sum / self.n as f64;
			(self.sum_sq / self.n as f64 - mean * mean).max(0.0)
		})
	}

	pub fn stddev_pop(&self) -> Option<f64> {
		self.variance_pop().map(f64::sqrt)
	}
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrdF64(f64);

impl OrdF64 {
	#[inline]
	pub fn new(value: f64) -> Option<Self> {
		(!value.is_nan()).then_some(Self(value))
	}

	#[inline]
	pub fn get(self) -> f64 {
		self.0
	}
}

impl PartialEq for OrdF64 {
	#[inline]
	fn eq(&self, other: &Self) -> bool {
		self.0.total_cmp(&other.0) == Ordering::Equal
	}
}

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
	#[inline]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for OrdF64 {
	#[inline]
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.total_cmp(&other.0)
	}
}

impl Hash for OrdF64 {
	#[inline]
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.to_bits().hash(state);
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Multiset<V: Ord> {
	counts: BTreeMap<V, u64>,
	total: u64,
}

impl<V: Ord> Default for Multiset<V> {
	fn default() -> Self {
		Self {
			counts: BTreeMap::new(),
			total: 0,
		}
	}
}

impl<V: Ord + Clone> Multiset<V> {
	pub fn add(&mut self, value: V) {
		*self.counts.entry(value).or_insert(0) += 1;
		self.total += 1;
	}

	pub fn remove(&mut self, value: &V) {
		let Some(count) = self.counts.get_mut(value) else {
			#[cfg(reifydb_assertions)]
			panic!("Multiset::remove of absent value");
			#[cfg(not(reifydb_assertions))]
			return;
		};
		*count -= 1;
		self.total -= 1;
		if *count == 0 {
			self.counts.remove(value);
		}
	}

	pub fn min(&self) -> Option<&V> {
		self.counts.keys().next()
	}

	pub fn max(&self) -> Option<&V> {
		self.counts.keys().next_back()
	}

	pub fn distinct(&self) -> usize {
		self.counts.len()
	}

	pub fn total(&self) -> u64 {
		self.total
	}

	pub fn is_empty(&self) -> bool {
		self.total == 0
	}

	pub fn mode(&self) -> Option<&V> {
		self.counts
			.iter()
			.reduce(|best, current| {
				if current.1 > best.1 {
					current
				} else {
					best
				}
			})
			.map(|(v, _)| v)
	}

	pub fn quantile(&self, q: f64) -> Option<&V> {
		if self.total == 0 {
			return None;
		}
		let q = q.clamp(0.0, 1.0);
		let rank = ((q * self.total as f64).ceil() as u64).clamp(1, self.total);
		let mut cumulative = 0u64;
		for (value, count) in &self.counts {
			cumulative += count;
			if cumulative >= rank {
				return Some(value);
			}
		}
		None
	}

	pub fn median(&self) -> Option<&V> {
		self.quantile(0.5)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedMap<K: Ord, V> {
	entries: BTreeMap<K, V>,
}

impl<K: Ord, V> Default for RetainedMap<K, V> {
	fn default() -> Self {
		Self {
			entries: BTreeMap::new(),
		}
	}
}

impl<K: Ord, V> RetainedMap<K, V> {
	pub fn insert(&mut self, key: K, value: V) {
		self.entries.insert(key, value);
	}

	pub fn remove(&mut self, key: &K) {
		self.entries.remove(key);
	}

	pub fn entries(&self) -> &BTreeMap<K, V> {
		&self.entries
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastValue<V> {
	value: Option<V>,
}

impl<V> Default for LastValue<V> {
	fn default() -> Self {
		Self {
			value: None,
		}
	}
}

impl<V: Clone> LastValue<V> {
	pub fn set(&mut self, value: V) {
		self.value = Some(value);
	}

	pub fn clear(&mut self) {
		self.value = None;
	}

	pub fn get(&self) -> Option<&V> {
		self.value.as_ref()
	}

	pub fn is_empty(&self) -> bool {
		self.value.is_none()
	}
}

impl<V: Clone + Debug> WindowAccumulator for LastValue<V>
where
	V: Serialize + DeserializeOwned + PartialEq,
{
	type Contribution = V;
	type Output = V;

	fn add(&mut self, contribution: &V) {
		self.value = Some(contribution.clone());
	}

	fn remove(&mut self, _contribution: &V) {
		self.value = None;
	}

	fn finalize(&self) -> Option<V> {
		self.value.clone()
	}

	fn is_empty(&self) -> bool {
		self.value.is_none()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointByCoord<C: Ord, V> {
	entries: BTreeMap<C, V>,
}

impl<C: Ord, V> Default for EndpointByCoord<C, V> {
	fn default() -> Self {
		Self {
			entries: BTreeMap::new(),
		}
	}
}

impl<C: Ord + Clone, V: Clone> EndpointByCoord<C, V> {
	pub fn observe(&mut self, coord: C, value: V) {
		self.entries.insert(coord, value);
	}

	pub fn forget(&mut self, coord: &C) {
		self.entries.remove(coord);
	}

	pub fn earliest(&self) -> Option<(&C, &V)> {
		self.entries.first_key_value()
	}

	pub fn earliest_value(&self) -> Option<&V> {
		self.entries.first_key_value().map(|(_, v)| v)
	}

	pub fn earliest_coord(&self) -> Option<&C> {
		self.entries.first_key_value().map(|(c, _)| c)
	}

	pub fn latest(&self) -> Option<(&C, &V)> {
		self.entries.last_key_value()
	}

	pub fn latest_value(&self) -> Option<&V> {
		self.entries.last_key_value().map(|(_, v)| v)
	}

	pub fn latest_coord(&self) -> Option<&C> {
		self.entries.last_key_value().map(|(c, _)| c)
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyedInvertibleAcc<K: Ord, A> {
	subs: BTreeMap<K, A>,
}

impl<K: Ord, A> Default for KeyedInvertibleAcc<K, A> {
	fn default() -> Self {
		Self {
			subs: BTreeMap::new(),
		}
	}
}

impl<K: Ord, A> KeyedInvertibleAcc<K, A> {
	pub fn entries(&self) -> &BTreeMap<K, A> {
		&self.subs
	}
}

impl<K, A> WindowAccumulator for KeyedInvertibleAcc<K, A>
where
	K: Ord + Clone + Debug + Serialize + DeserializeOwned,
	A: WindowAccumulator,
{
	type Contribution = (K, A::Contribution);
	type Output = BTreeMap<K, A::Output>;

	fn add(&mut self, contribution: &(K, A::Contribution)) {
		self.subs.entry(contribution.0.clone()).or_default().add(&contribution.1);
	}

	fn remove(&mut self, contribution: &(K, A::Contribution)) {
		if let Some(sub) = self.subs.get_mut(&contribution.0) {
			sub.remove(&contribution.1);
			if sub.is_empty() {
				self.subs.remove(&contribution.0);
			}
		}
	}

	fn finalize(&self) -> Option<BTreeMap<K, A::Output>> {
		if self.subs.is_empty() {
			return None;
		}
		let out: BTreeMap<K, A::Output> =
			self.subs.iter().filter_map(|(k, s)| s.finalize().map(|v| (k.clone(), v))).collect();
		(!out.is_empty()).then_some(out)
	}

	fn is_empty(&self) -> bool {
		self.subs.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetainedAcc<K: Ord, V> {
	map: RetainedMap<K, V>,
}

impl<K: Ord, V> Default for RetainedAcc<K, V> {
	fn default() -> Self {
		Self {
			map: RetainedMap::default(),
		}
	}
}

impl<K, V> WindowAccumulator for RetainedAcc<K, V>
where
	K: Ord + Clone + Debug + Serialize + DeserializeOwned,
	V: Clone + Debug + PartialEq + Serialize + DeserializeOwned,
{
	type Contribution = (K, V);
	type Output = BTreeMap<K, V>;

	fn add(&mut self, contribution: &(K, V)) {
		self.map.insert(contribution.0.clone(), contribution.1.clone());
	}

	fn remove(&mut self, contribution: &(K, V)) {
		self.map.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<BTreeMap<K, V>> {
		(!self.map.is_empty()).then(|| self.map.entries().clone())
	}

	fn is_empty(&self) -> bool {
		self.map.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
pub struct SealingMax<C: Slot, V: Ord> {
	lateness: Option<C::Duration>,
	high_water: Option<C>,
	sealed: Option<V>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V: Ord> Default for SealingMax<C, V> {
	fn default() -> Self {
		Self {
			lateness: None,
			high_water: None,
			sealed: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V: Ord + Clone> SealingMax<C, V> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			lateness: Some(lateness),
			high_water: None,
			sealed: None,
			tail: BTreeMap::new(),
		}
	}

	fn seal_aged(&mut self) {
		let (Some(hw), Some(l)) = (self.high_water, self.lateness) else {
			return;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw - c > l {
				let (_, v) = self.tail.pop_first().expect("non-empty");
				self.sealed = Some(match self.sealed.take() {
					Some(s) => s.max(v),
					None => v,
				});
			} else {
				break;
			}
		}
	}

	pub fn max(&self) -> Option<V> {
		let tail_max = self.tail.values().max().cloned();
		match (self.sealed.clone(), tail_max) {
			(Some(s), Some(t)) => Some(s.max(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
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
		let coord = contribution.0;
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, contribution.1.clone());
		self.seal_aged();
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.tail.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.max()
	}

	fn is_empty(&self) -> bool {
		self.sealed.is_none() && self.tail.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
pub struct SealingMin<C: Slot, V: Ord> {
	lateness: Option<C::Duration>,
	high_water: Option<C>,
	sealed: Option<V>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V: Ord> Default for SealingMin<C, V> {
	fn default() -> Self {
		Self {
			lateness: None,
			high_water: None,
			sealed: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V: Ord + Clone> SealingMin<C, V> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			lateness: Some(lateness),
			high_water: None,
			sealed: None,
			tail: BTreeMap::new(),
		}
	}

	fn seal_aged(&mut self) {
		let (Some(hw), Some(l)) = (self.high_water, self.lateness) else {
			return;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw - c > l {
				let (_, v) = self.tail.pop_first().expect("non-empty");
				self.sealed = Some(match self.sealed.take() {
					Some(s) => s.min(v),
					None => v,
				});
			} else {
				break;
			}
		}
	}

	pub fn min(&self) -> Option<V> {
		let tail_min = self.tail.values().min().cloned();
		match (self.sealed.clone(), tail_min) {
			(Some(s), Some(t)) => Some(s.min(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
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
		let coord = contribution.0;
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, contribution.1.clone());
		self.seal_aged();
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.tail.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.min()
	}

	fn is_empty(&self) -> bool {
		self.sealed.is_none() && self.tail.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
pub struct SealingEndpoint<C: Slot, V> {
	lateness: Option<C::Duration>,
	high_water: Option<C>,
	sealed_open: Option<(C, V)>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V> Default for SealingEndpoint<C, V> {
	fn default() -> Self {
		Self {
			lateness: None,
			high_water: None,
			sealed_open: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V: Clone> SealingEndpoint<C, V> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			lateness: Some(lateness),
			high_water: None,
			sealed_open: None,
			tail: BTreeMap::new(),
		}
	}

	fn seal_aged(&mut self) {
		let (Some(hw), Some(l)) = (self.high_water, self.lateness) else {
			return;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw - c > l {
				let (c, v) = self.tail.pop_first().expect("non-empty");
				self.sealed_open = Some(match self.sealed_open.take() {
					Some((sc, sv)) if sc <= c => (sc, sv),
					_ => (c, v),
				});
			} else {
				break;
			}
		}
	}

	pub fn open(&self) -> Option<&V> {
		match &self.sealed_open {
			Some((_, v)) => Some(v),
			None => self.tail.values().next(),
		}
	}

	pub fn close(&self) -> Option<&V> {
		match self.tail.values().next_back() {
			Some(v) => Some(v),
			None => self.sealed_open.as_ref().map(|(_, v)| v),
		}
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
		let coord = contribution.0;
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, contribution.1.clone());
		self.seal_aged();
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.tail.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<(V, V)> {
		match (self.open(), self.close()) {
			(Some(o), Some(c)) => Some((o.clone(), c.clone())),
			_ => None,
		}
	}

	fn is_empty(&self) -> bool {
		self.sealed_open.is_none() && self.tail.is_empty()
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
	lateness: Option<C::Duration>,
	high_water: Option<C>,
	sealed: F::State,
	last_sealed: Option<F::Value>,
	tail: BTreeMap<C, F::Value>,
	#[serde(skip)]
	marker: PhantomData<fn() -> F>,
}

impl<C: Slot, F: SealFold> Clone for SealingFold<C, F> {
	fn clone(&self) -> Self {
		Self {
			lateness: self.lateness,
			high_water: self.high_water,
			sealed: self.sealed.clone(),
			last_sealed: self.last_sealed.clone(),
			tail: self.tail.clone(),
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> Debug for SealingFold<C, F> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("SealingFold")
			.field("lateness", &self.lateness)
			.field("high_water", &self.high_water)
			.field("sealed", &self.sealed)
			.field("last_sealed", &self.last_sealed)
			.field("tail", &self.tail)
			.finish()
	}
}

impl<C: Slot, F: SealFold> Default for SealingFold<C, F> {
	fn default() -> Self {
		Self {
			lateness: None,
			high_water: None,
			sealed: F::State::default(),
			last_sealed: None,
			tail: BTreeMap::new(),
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> SealingFold<C, F> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			lateness: Some(lateness),
			high_water: None,
			sealed: F::State::default(),
			last_sealed: None,
			tail: BTreeMap::new(),
			marker: PhantomData,
		}
	}

	fn seal_aged(&mut self) {
		let (Some(hw), Some(l)) = (self.high_water, self.lateness) else {
			return;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw - c > l {
				let (_, v) = self.tail.pop_first().expect("non-empty");
				F::fold(&mut self.sealed, self.last_sealed.as_ref(), &v);
				self.last_sealed = Some(v);
			} else {
				break;
			}
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
		let coord = contribution.0;
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, contribution.1.clone());
		self.seal_aged();
	}

	fn remove(&mut self, contribution: &(C, F::Value)) {
		self.tail.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<F::Output> {
		let mut state = self.sealed.clone();
		let mut prev = self.last_sealed.clone();
		for v in self.tail.values() {
			F::fold(&mut state, prev.as_ref(), v);
			prev = Some(v.clone());
		}
		F::output(&state)
	}

	fn is_empty(&self) -> bool {
		self.last_sealed.is_none() && self.tail.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
pub struct SealingTail<C: Slot, V> {
	lateness: Option<C::Duration>,
	high_water: Option<C>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V> Default for SealingTail<C, V> {
	fn default() -> Self {
		Self {
			lateness: None,
			high_water: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V: Clone> SealingTail<C, V> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			lateness: Some(lateness),
			high_water: None,
			tail: BTreeMap::new(),
		}
	}

	fn seal_aged(&mut self) {
		let (Some(hw), Some(l)) = (self.high_water, self.lateness) else {
			return;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw - c > l {
				self.tail.pop_first();
			} else {
				break;
			}
		}
	}

	pub fn add(&mut self, coord: C, value: V) {
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, value);
		self.seal_aged();
	}

	pub fn remove(&mut self, coord: &C) {
		self.tail.remove(coord);
	}

	pub fn tail(&self) -> &BTreeMap<C, V> {
		&self.tail
	}

	pub fn is_empty(&self) -> bool {
		self.tail.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, C::Duration: Serialize, V: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, C::Duration: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned"
))]
pub struct TailAcc<C: Slot, V> {
	events: SealingTail<C, V>,
}

impl<C: Slot, V> Default for TailAcc<C, V> {
	fn default() -> Self {
		Self {
			events: SealingTail::default(),
		}
	}
}

impl<C: Slot, V: Clone> TailAcc<C, V> {
	pub fn with_lateness(lateness: C::Duration) -> Self {
		Self {
			events: SealingTail::with_lateness(lateness),
		}
	}
}

impl<C, V> WindowAccumulator for TailAcc<C, V>
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

#[cfg(test)]
mod tests {
	use postcard::{from_bytes, to_allocvec};
	use serde::{Deserialize, Serialize};

	use super::*;

	// Test-fixture accumulators that exercise the three storage families
	// through the WindowAccumulator trait: invertible (SumAcc via Moments),
	// ordered multiset (MinAcc via Multiset), and keyed-retained (LastAcc
	// via RetainedMap). The generic property helpers below run over these.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct SumAcc {
		moments: Moments,
	}

	impl WindowAccumulator for SumAcc {
		type Contribution = f64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &f64) {
			self.moments.add(*contribution);
		}

		fn remove(&mut self, contribution: &f64) {
			self.moments.remove(*contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			(!self.moments.is_empty()).then(|| OrdF64::new(self.moments.sum()).expect("finite sum"))
		}

		fn is_empty(&self) -> bool {
			self.moments.is_empty()
		}
	}

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct MinAcc {
		values: Multiset<OrdF64>,
	}

	impl WindowAccumulator for MinAcc {
		type Contribution = OrdF64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &OrdF64) {
			self.values.add(*contribution);
		}

		fn remove(&mut self, contribution: &OrdF64) {
			self.values.remove(contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			self.values.min().copied()
		}

		fn is_empty(&self) -> bool {
			self.values.is_empty()
		}
	}

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct LastAcc {
		retained: RetainedMap<u64, i64>,
	}

	impl WindowAccumulator for LastAcc {
		type Contribution = (u64, i64);
		type Output = i64;

		fn add(&mut self, contribution: &(u64, i64)) {
			self.retained.insert(contribution.0, contribution.1);
		}

		fn remove(&mut self, contribution: &(u64, i64)) {
			self.retained.remove(&contribution.0);
		}

		fn finalize(&self) -> Option<i64> {
			self.retained.entries().last_key_value().map(|(_, v)| *v)
		}

		fn is_empty(&self) -> bool {
			self.retained.is_empty()
		}
	}

	fn of64(v: f64) -> OrdF64 {
		OrdF64::new(v).expect("not nan")
	}

	// The load-bearing contract: applying a contribution and then removing
	// it must leave finalize() exactly where it started. This is what lets
	// the driver process an Update as remove(pre)+add(post) and a Remove as
	// remove(pre) without retaining per-event history. The probe's identity
	// must be absent from the initial state (for keyed-retained accumulators
	// an add over an existing key replaces rather than stacks - see
	// retained_add_over_existing_key_then_remove_deletes).
	fn assert_add_remove_is_inverse<A: WindowAccumulator>(initial: &[A::Contribution], probe: A::Contribution) {
		let mut acc = A::default();
		for c in initial {
			acc.add(c);
		}
		let before = acc.finalize();
		acc.add(&probe);
		acc.remove(&probe);
		assert_eq!(acc.finalize(), before, "add then remove must restore finalize()");
	}

	// For commutative families, the multiset of contributions determines the
	// result regardless of arrival order.
	fn assert_order_independent<A>(contributions: &[A::Contribution])
	where
		A: WindowAccumulator,
	{
		let mut forward = A::default();
		for c in contributions {
			forward.add(c);
		}
		let mut backward = A::default();
		for c in contributions.iter().rev() {
			backward.add(c);
		}
		assert_eq!(forward.finalize(), backward.finalize(), "finalize() must be order-independent");
	}

	#[test]
	fn sum_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<SumAcc>(&[1.0, 2.0, 3.0], 7.0);
	}

	#[test]
	fn min_add_remove_is_inverse_even_when_probe_is_new_minimum() {
		// Adding a value below the current minimum then removing it must
		// restore the old minimum - the multiset case a scalar running-min
		// cannot do.
		assert_add_remove_is_inverse::<MinAcc>(&[of64(5.0), of64(8.0), of64(6.0)], of64(1.0));
	}

	#[test]
	fn min_add_remove_is_inverse_for_duplicate_value() {
		// Removing one occurrence of a duplicated value must not drop the
		// value entirely while another occurrence remains.
		assert_add_remove_is_inverse::<MinAcc>(&[of64(5.0), of64(5.0), of64(8.0)], of64(5.0));
	}

	#[test]
	fn retained_add_remove_is_inverse_for_fresh_key() {
		assert_add_remove_is_inverse::<LastAcc>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	}

	#[test]
	fn sum_is_order_independent() {
		assert_order_independent::<SumAcc>(&[1.0, 2.0, 4.0, 8.0]);
	}

	#[test]
	fn min_is_order_independent() {
		assert_order_independent::<MinAcc>(&[of64(3.0), of64(1.0), of64(4.0), of64(1.0), of64(5.0)]);
	}

	#[test]
	fn retained_is_order_independent_for_distinct_keys() {
		// Last-write-wins is only order-independent when keys are distinct;
		// colliding keys legitimately depend on order (later write wins).
		assert_order_independent::<LastAcc>(&[(1u64, 10i64), (2, 20), (3, 30)]);
	}

	#[test]
	fn retained_add_over_existing_key_then_remove_deletes() {
		// Documents the keyed-replace semantics: an add over an existing
		// key overwrites it, so a following remove deletes the entry rather
		// than restoring the prior value. The driver never does this for a
		// single diff; Update routing is remove(pre)+add(post).
		let mut acc = LastAcc::default();
		acc.add(&(1u64, 10i64));
		acc.add(&(1u64, 99i64));
		acc.remove(&(1u64, 99i64));
		assert!(acc.is_empty());
		assert_eq!(acc.finalize(), None);
	}

	#[test]
	fn moments_drains_to_exact_zero() {
		let mut m = Moments::default();
		m.add(0.1);
		m.add(0.2);
		m.remove(0.1);
		m.remove(0.2);
		assert_eq!(m.count(), 0);
		assert_eq!(m.sum(), 0.0, "fully drained accumulator resets sum to exact zero");
		assert!(m.is_empty());
		assert_eq!(m.mean(), None);
		assert_eq!(m.variance_pop(), None);
	}

	#[test]
	fn moments_mean_and_variance() {
		let mut m = Moments::default();
		for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
			m.add(x);
		}
		assert_eq!(m.count(), 8);
		assert_eq!(m.mean(), Some(5.0));
		assert_eq!(m.variance_pop(), Some(4.0));
		assert_eq!(m.stddev_pop(), Some(2.0));
	}

	#[test]
	fn multiset_min_max_distinct_total() {
		let mut ms: Multiset<u64> = Multiset::default();
		for v in [5u64, 1, 5, 9, 1] {
			ms.add(v);
		}
		assert_eq!(ms.min(), Some(&1));
		assert_eq!(ms.max(), Some(&9));
		assert_eq!(ms.distinct(), 3);
		assert_eq!(ms.total(), 5);

		ms.remove(&1);
		assert_eq!(ms.min(), Some(&1), "one occurrence of 1 remains");
		assert_eq!(ms.distinct(), 3);
		ms.remove(&1);
		assert_eq!(ms.min(), Some(&5), "last occurrence of 1 removed, min rises");
		assert_eq!(ms.distinct(), 2);
	}

	#[test]
	fn multiset_quantile_and_median_nearest_rank() {
		let mut ms: Multiset<u64> = Multiset::default();
		for v in [1u64, 2, 3, 4, 5] {
			ms.add(v);
		}
		assert_eq!(ms.quantile(0.0), Some(&1));
		assert_eq!(ms.median(), Some(&3));
		assert_eq!(ms.quantile(1.0), Some(&5));
		assert_eq!(ms.quantile(0.5), Some(&3));
	}

	#[test]
	fn multiset_mode_breaks_ties_to_smallest_value() {
		let mut ms: Multiset<u64> = Multiset::default();
		for v in [7u64, 7, 3, 3, 9] {
			ms.add(v);
		}
		assert_eq!(ms.mode(), Some(&3), "3 and 7 tie at count 2; smallest wins deterministically");
	}

	#[test]
	fn ordf64_total_order_and_nan_rejection() {
		assert!(OrdF64::new(f64::NAN).is_none());
		assert!(of64(-1.0) < of64(0.0));
		assert!(of64(0.0) < of64(1.0));
		let mut ms: Multiset<OrdF64> = Multiset::default();
		ms.add(of64(2.5));
		ms.add(of64(-3.0));
		ms.add(of64(2.5));
		assert_eq!(ms.min(), Some(&of64(-3.0)));
		assert_eq!(ms.max(), Some(&of64(2.5)));
		assert_eq!(ms.total(), 3);
	}

	#[test]
	fn moments_postcard_roundtrip() {
		let mut m = Moments::default();
		m.add(1.5);
		m.add(2.5);
		let bytes = to_allocvec(&m).expect("serialize");
		let restored: Moments = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, m);
	}

	#[test]
	fn multiset_postcard_roundtrip() {
		let mut ms: Multiset<OrdF64> = Multiset::default();
		ms.add(of64(1.0));
		ms.add(of64(1.0));
		ms.add(of64(2.0));
		let bytes = to_allocvec(&ms).expect("serialize");
		let restored: Multiset<OrdF64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, ms);
		assert_eq!(restored.min(), Some(&of64(1.0)));
		assert_eq!(restored.total(), 3);
	}

	#[test]
	fn retained_map_postcard_roundtrip() {
		let mut rm: RetainedMap<u64, i64> = RetainedMap::default();
		rm.insert(1, 10);
		rm.insert(2, 20);
		let bytes = to_allocvec(&rm).expect("serialize");
		let restored: RetainedMap<u64, i64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, rm);
		assert_eq!(restored.len(), 2);
	}

	#[test]
	fn last_value_is_last_write_wins() {
		// Matches the per-window last-write-wins contract: add overwrites,
		// remove clears. add(v1) then add(v2) keeps v2; remove empties.
		let mut lv: LastValue<i64> = LastValue::default();
		assert!(lv.is_empty());
		lv.add(&10);
		lv.add(&20);
		assert_eq!(lv.finalize(), Some(20));
		lv.remove(&20);
		assert!(lv.is_empty());
		assert_eq!(lv.finalize(), None);
	}

	#[test]
	fn endpoint_by_coord_tracks_both_ends_and_is_removal_safe() {
		// `earliest`/`latest` are the values at the smallest/largest
		// coordinate; forgetting the current end must reveal the prior one
		// (removal-safe). This is what lets OHLCV derive open (earliest)
		// and close (latest) from ONE removal-safe structure.
		let mut ends: EndpointByCoord<u64, i64> = EndpointByCoord::default();
		ends.observe(10, 100);
		ends.observe(30, 300);
		ends.observe(20, 200);
		assert_eq!(ends.earliest(), Some((&10, &100)));
		assert_eq!(ends.earliest_coord(), Some(&10));
		assert_eq!(ends.latest(), Some((&30, &300)));
		assert_eq!(ends.latest_coord(), Some(&30));

		ends.forget(&10);
		assert_eq!(ends.earliest(), Some((&20, &200)), "forgetting the min reveals the prior min");
		ends.forget(&30);
		assert_eq!(ends.latest(), Some((&20, &200)), "forgetting the max reveals the prior max");

		// Re-observing an existing coordinate replaces its value in place.
		ends.observe(20, 999);
		assert_eq!(ends.earliest_value(), Some(&999));
		assert_eq!(ends.latest_value(), Some(&999));

		ends.forget(&20);
		assert!(ends.is_empty());
		assert_eq!(ends.earliest(), None);
		assert_eq!(ends.latest(), None);
	}

	#[test]
	fn retained_acc_add_remove_is_inverse_for_fresh_key() {
		assert_add_remove_is_inverse::<RetainedAcc<u64, i64>>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	}

	#[test]
	fn retained_acc_is_order_independent_for_distinct_keys() {
		assert_order_independent::<RetainedAcc<u64, i64>>(&[(1u64, 10i64), (2, 20), (3, 30)]);
	}

	#[test]
	fn retained_acc_finalize_returns_whole_map() {
		// Unlike LastValue/LatestByCoord, the carry-forward family needs the
		// FULL retained map (e.g. TWAP integrates every observation), so
		// finalize yields a clone of all entries, not just the latest.
		let mut acc: RetainedAcc<u64, i64> = RetainedAcc::default();
		assert!(acc.is_empty());
		assert_eq!(acc.finalize(), None);
		acc.add(&(2, 20));
		acc.add(&(1, 10));
		let map = acc.finalize().expect("non-empty");
		assert_eq!(map.len(), 2);
		assert_eq!(map.get(&1), Some(&10));
		assert_eq!(map.get(&2), Some(&20));
	}

	#[test]
	fn retained_acc_add_over_existing_key_then_remove_deletes() {
		// Keyed-replace semantics: an add over an existing key overwrites
		// it, so a following remove deletes the entry rather than restoring
		// the prior value. The driver never does this for a single diff -
		// Update routing is remove(pre)+add(post) on distinct calls.
		let mut acc: RetainedAcc<u64, i64> = RetainedAcc::default();
		acc.add(&(1, 10));
		acc.add(&(1, 99));
		acc.remove(&(1, 99));
		assert!(acc.is_empty());
		assert_eq!(acc.finalize(), None);
	}

	#[test]
	fn retained_acc_postcard_roundtrip() {
		let mut acc: RetainedAcc<u64, i64> = RetainedAcc::default();
		acc.add(&(1, 10));
		acc.add(&(2, 20));
		let bytes = to_allocvec(&acc).expect("serialize");
		let restored: RetainedAcc<u64, i64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, acc);
	}

	#[test]
	fn keyed_invertible_routes_per_key_and_drops_empty_keys() {
		// Each key holds its own invertible sub-accumulator; removing a
		// key's last contribution drops the key entirely, so finalize never
		// reports a key whose sub-accumulator drained to empty. This is the
		// O(distinct keys) shape behind volume-profile and top-K.
		let mut acc: KeyedInvertibleAcc<u64, Moments> = KeyedInvertibleAcc::default();
		assert!(acc.is_empty());
		assert_eq!(acc.finalize(), None);

		acc.add(&(1, 10.0));
		acc.add(&(1, 20.0));
		acc.add(&(2, 5.0));
		let out = acc.finalize().expect("non-empty");
		assert_eq!(out.len(), 2);
		assert_eq!(out.get(&1).map(|m| m.sum()), Some(30.0));
		assert_eq!(out.get(&2).map(|m| m.sum()), Some(5.0));

		acc.remove(&(2, 5.0));
		let out = acc.finalize().expect("non-empty");
		assert_eq!(out.len(), 1, "key 2 drained to empty and was dropped");
		assert!(out.get(&2).is_none());
	}

	#[test]
	fn keyed_invertible_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<KeyedInvertibleAcc<u64, Moments>>(
			&[(1u64, 10.0f64), (2, 20.0), (1, 30.0)],
			(3u64, 7.0f64),
		);
	}

	#[test]
	fn keyed_invertible_postcard_roundtrip() {
		let mut acc: KeyedInvertibleAcc<u64, Moments> = KeyedInvertibleAcc::default();
		acc.add(&(1, 10.0));
		acc.add(&(2, 20.0));
		let bytes = to_allocvec(&acc).expect("serialize");
		let restored: KeyedInvertibleAcc<u64, Moments> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, acc);
	}

	#[test]
	fn sealing_max_seals_aged_and_keeps_recent_tail_removal_safe() {
		// lateness 10: an event aged past hw-10 is folded into the O(1)
		// sealed scalar; events within 10 of the high-water stay in the
		// removable tail.
		let mut acc: SealingMax<u64, i64> = SealingMax::with_lateness(10);
		acc.add(&(0, 5));
		acc.add(&(5, 8));
		acc.add(&(12, 3)); // hw=12; coord 0 (age 12>10) seals -> sealed=5, tail={5:8,12:3}
		assert_eq!(acc.max(), Some(8));

		// Removing the aged (sealed) event is a dropped no-op: the sealed
		// max cannot be lowered (the bounded-lateness assumption).
		acc.remove(&(0, 5));
		assert_eq!(acc.max(), Some(8), "aged removal does not disturb the sealed max");

		// Removing a still-live tail event IS removal-safe; the max falls
		// back to max(sealed, remaining tail).
		acc.remove(&(5, 8));
		assert_eq!(acc.max(), Some(5), "tail max 8 removed; falls back to sealed 5");
	}

	#[test]
	fn sealing_min_seals_aged_extreme() {
		let mut acc: SealingMin<u64, i64> = SealingMin::with_lateness(10);
		acc.add(&(0, 2));
		acc.add(&(5, 9));
		acc.add(&(12, 7)); // coord 0 seals -> sealed=2, tail={5:9,12:7}
		assert_eq!(acc.min(), Some(2));
		acc.remove(&(5, 9));
		assert_eq!(acc.min(), Some(2), "sealed min 2 survives removal of a live event");
	}

	#[test]
	fn sealing_max_default_never_seals_and_is_fully_invertible() {
		// With no lateness bound, nothing seals, so add/remove is a pure
		// inverse - the Default-constructed sealing acc behaves exactly like
		// the removal-safe decompose variant.
		assert_add_remove_is_inverse::<SealingMax<u64, i64>>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
		let mut acc: SealingMax<u64, i64> = SealingMax::default();
		acc.add(&(0, 5));
		acc.add(&(100, 8)); // far apart but never sealed without a lateness bound
		acc.remove(&(100, 8));
		assert_eq!(acc.max(), Some(5), "removing the max reveals the prior max (no sealing)");
	}

	#[test]
	fn sealing_endpoint_freezes_open_and_tracks_live_close() {
		let mut acc: SealingEndpoint<u64, i64> = SealingEndpoint::with_lateness(10);
		acc.add(&(0, 100));
		acc.add(&(5, 200));
		acc.add(&(12, 300)); // coord 0 ages out -> sealed_open=(0,100); tail={5:200,12:300}
		assert_eq!(acc.open(), Some(&100), "open frozen to the earliest observation");
		assert_eq!(acc.close(), Some(&300), "close is the latest live observation");

		acc.remove(&(0, 100));
		assert_eq!(acc.open(), Some(&100), "aged open removal is a dropped no-op (frozen)");

		acc.remove(&(12, 300));
		assert_eq!(acc.close(), Some(&200), "removing the latest reveals the prior latest in the tail");

		acc.add(&(20, 400)); // coord 5 ages out; sealed_open stays (0,100) since 0 <= 5
		assert_eq!(acc.open(), Some(&100));
		assert_eq!(acc.close(), Some(&400));
	}

	#[test]
	fn sealing_endpoint_default_is_fully_invertible() {
		assert_add_remove_is_inverse::<SealingEndpoint<u64, i64>>(&[(1u64, 10i64), (3, 30)], (2u64, 20i64));
	}

	#[test]
	fn sealing_primitives_postcard_roundtrip() {
		let mut mx: SealingMax<u64, i64> = SealingMax::with_lateness(10);
		mx.add(&(0, 5));
		mx.add(&(12, 8));
		let bytes = to_allocvec(&mx).expect("serialize");
		let restored: SealingMax<u64, i64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, mx);

		let mut ep: SealingEndpoint<u64, i64> = SealingEndpoint::with_lateness(10);
		ep.add(&(0, 100));
		ep.add(&(12, 300));
		let bytes = to_allocvec(&ep).expect("serialize");
		let restored: SealingEndpoint<u64, i64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, ep);
	}

	// A left-fold over consecutive observations in coordinate order: the path
	// length sum(|cur - prev|). The first observation has no predecessor and
	// contributes 0. This is the order-dependent, non-invertible shape that
	// SealingFold seals the aged prefix of while keeping the recent tail
	// removable.
	struct AbsPathFold;

	impl SealFold for AbsPathFold {
		type Value = f64;
		type State = f64;
		type Output = f64;

		fn fold(state: &mut f64, prev: Option<&f64>, cur: &f64) {
			if let Some(p) = prev {
				*state += (cur - p).abs();
			}
		}

		fn output(state: &f64) -> Option<f64> {
			Some(*state)
		}
	}

	#[test]
	fn sealing_fold_no_lateness_sums_all_adjacent_steps() {
		let mut acc: SealingFold<u64, AbsPathFold> = SealingFold::default();
		acc.add(&(0, 10.0));
		acc.add(&(1, 20.0));
		acc.add(&(2, 15.0));
		// |20-10| + |15-20| = 10 + 5 = 15; the first observation contributes 0.
		assert_eq!(acc.finalize(), Some(15.0));
	}

	#[test]
	fn sealing_fold_seals_aged_prefix_exactly_for_forward_data() {
		// lateness 1: observation at coord 0 ages out once hw reaches 2 and is
		// folded into the sealed path; the total is still exact for
		// forward-only inserts.
		let mut acc: SealingFold<u64, AbsPathFold> = SealingFold::with_lateness(1);
		acc.add(&(0, 10.0));
		acc.add(&(1, 20.0)); // coord 0 still live (hw-0 = 1, not > 1)
		acc.add(&(2, 15.0)); // coord 0 ages out -> sealed
		assert_eq!(acc.finalize(), Some(15.0), "sealed prefix preserves the full path exactly");
	}

	#[test]
	fn sealing_fold_aged_removal_is_dropped_no_op_but_live_removal_is_safe() {
		let mut acc: SealingFold<u64, AbsPathFold> = SealingFold::with_lateness(1);
		acc.add(&(0, 10.0));
		acc.add(&(1, 20.0));
		acc.add(&(2, 15.0)); // coord 0 sealed
		// Removing the aged (sealed) observation is a dropped no-op.
		acc.remove(&(0, 10.0));
		assert_eq!(acc.finalize(), Some(15.0), "aged removal does not disturb the sealed path");
		// Removing a still-live tail observation IS removal-safe: the path
		// recomputes over sealed-prefix + remaining tail (10 -> 20 = 10).
		acc.remove(&(2, 15.0));
		assert_eq!(acc.finalize(), Some(10.0), "live removal recomputes the path");
	}

	#[test]
	fn sealing_fold_default_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<SealingFold<u64, AbsPathFold>>(
			&[(0u64, 10.0f64), (1, 20.0)],
			(2u64, 30.0f64),
		);
	}

	#[test]
	fn sealing_fold_postcard_roundtrip() {
		let mut acc: SealingFold<u64, AbsPathFold> = SealingFold::with_lateness(1);
		acc.add(&(0, 10.0));
		acc.add(&(1, 20.0));
		acc.add(&(2, 15.0));
		let bytes = to_allocvec(&acc).expect("serialize");
		let restored: SealingFold<u64, AbsPathFold> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored.finalize(), acc.finalize());
	}

	#[test]
	fn sealing_tail_drops_aged_keeps_recent() {
		let mut tail: SealingTail<u64, i64> = SealingTail::with_lateness(10);
		tail.add(0, 1);
		tail.add(5, 2);
		tail.add(12, 3); // hw=12; coord 0 (age 12 > 10) dropped
		let keys: Vec<u64> = tail.tail().keys().copied().collect();
		assert_eq!(keys, vec![5, 12], "aged prefix dropped, recent tail kept in order");
		tail.remove(&5);
		let keys: Vec<u64> = tail.tail().keys().copied().collect();
		assert_eq!(keys, vec![12], "live tail entry removable");
	}

	#[test]
	fn sealing_tail_default_never_drops() {
		let mut tail: SealingTail<u64, i64> = SealingTail::default();
		tail.add(0, 1);
		tail.add(100, 2);
		assert_eq!(tail.tail().len(), 2, "with no lateness bound nothing is dropped");
	}

	#[test]
	fn sealing_tail_postcard_roundtrip() {
		let mut tail: SealingTail<u64, i64> = SealingTail::with_lateness(10);
		tail.add(0, 1);
		tail.add(12, 3);
		let bytes = to_allocvec(&tail).expect("serialize");
		let restored: SealingTail<u64, i64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, tail);
	}

	#[test]
	fn tail_acc_no_lateness_retains_whole_window_like_retained_acc() {
		// With no lateness bound, TailAcc.finalize() returns the full map -
		// a drop-in for RetainedAcc (same Output = BTreeMap<C, V>).
		let mut acc: TailAcc<u64, i64> = TailAcc::default();
		acc.add(&(0, 10));
		acc.add(&(100, 20));
		let map = acc.finalize().expect("non-empty");
		assert_eq!(map.len(), 2);
		assert_eq!(map.get(&0), Some(&10));
		assert_eq!(map.get(&100), Some(&20));
	}

	#[test]
	fn tail_acc_default_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<TailAcc<u64, i64>>(&[(0u64, 10i64), (1, 20)], (2u64, 30i64));
	}

	#[test]
	fn tail_acc_with_lateness_drops_aged_from_finalize() {
		let mut acc: TailAcc<u64, i64> = TailAcc::with_lateness(10);
		acc.add(&(0, 10));
		acc.add(&(5, 20));
		acc.add(&(12, 30)); // hw=12; coord 0 (age 12 > 10) dropped
		let map = acc.finalize().expect("non-empty");
		assert_eq!(
			map.keys().copied().collect::<Vec<_>>(),
			vec![5, 12],
			"aged prefix dropped from the emitted map"
		);
	}

	#[test]
	fn tail_acc_postcard_roundtrip() {
		let mut acc: TailAcc<u64, i64> = TailAcc::with_lateness(10);
		acc.add(&(0, 1));
		acc.add(&(12, 3));
		let bytes = to_allocvec(&acc).expect("serialize");
		let restored: TailAcc<u64, i64> = from_bytes(&bytes).expect("deserialize");
		assert_eq!(restored, acc);
	}
}
