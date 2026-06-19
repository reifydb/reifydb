// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::BTreeMap,
	fmt::Debug,
	hash::{Hash, Hasher},
};

use reifydb_value::reifydb_assertions;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::WindowAccumulator;

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
		reifydb_assertions! {
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

	pub fn merge(&mut self, other: &Self) {
		for (value, count) in &other.counts {
			*self.counts.entry(value.clone()).or_insert(0) += count;
			self.total += count;
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
pub struct KeyedInvertibleAccumulator<K: Ord, A> {
	subs: BTreeMap<K, A>,
}

impl<K: Ord, A> Default for KeyedInvertibleAccumulator<K, A> {
	fn default() -> Self {
		Self {
			subs: BTreeMap::new(),
		}
	}
}

impl<K: Ord, A> KeyedInvertibleAccumulator<K, A> {
	pub fn entries(&self) -> &BTreeMap<K, A> {
		&self.subs
	}
}

impl<K, A> WindowAccumulator for KeyedInvertibleAccumulator<K, A>
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
pub struct RetainedAccumulator<K: Ord, V> {
	map: RetainedMap<K, V>,
}

impl<K: Ord, V> Default for RetainedAccumulator<K, V> {
	fn default() -> Self {
		Self {
			map: RetainedMap::default(),
		}
	}
}

impl<K, V> WindowAccumulator for RetainedAccumulator<K, V>
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
