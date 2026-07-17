// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	mem,
	ops::{Add, Rem, Sub},
};

use reifydb_core::{
	util::memory::HeapSize,
	window::{
		accumulator::{
			WindowAccumulator,
			invertible::Multiset,
			sealing::{SealingEndpoint, SealingMax, SealingMin},
		},
		span::Slot,
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		duration::Duration,
		number::safe::{add::SafeAdd, div::SafeDiv, sub::SafeSub},
	},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WindowSlotKey {
	pub timestamp: DateTime,
	pub seq: u64,
}

impl WindowSlotKey {
	pub fn new(timestamp: DateTime, seq: u64) -> Self {
		Self {
			timestamp,
			seq,
		}
	}
}

impl Add<Duration> for WindowSlotKey {
	type Output = WindowSlotKey;
	fn add(self, duration: Duration) -> WindowSlotKey {
		WindowSlotKey {
			timestamp: self.timestamp + duration,
			seq: self.seq,
		}
	}
}

impl Sub<WindowSlotKey> for WindowSlotKey {
	type Output = Duration;
	fn sub(self, other: WindowSlotKey) -> Duration {
		self.timestamp - other.timestamp
	}
}

impl Sub<Duration> for WindowSlotKey {
	type Output = WindowSlotKey;
	fn sub(self, duration: Duration) -> WindowSlotKey {
		WindowSlotKey {
			timestamp: self.timestamp - duration,
			seq: self.seq,
		}
	}
}

impl Rem<Duration> for WindowSlotKey {
	type Output = Duration;
	fn rem(self, duration: Duration) -> Duration {
		self.timestamp % duration
	}
}

impl Slot for WindowSlotKey {
	type Duration = Duration;

	fn order_key(&self) -> u64 {
		self.timestamp.to_nanos()
	}

	fn from_order_key(order_key: u64) -> Self {
		WindowSlotKey {
			timestamp: DateTime::from_nanos(order_key),
			seq: 0,
		}
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggregateSlot {
	Count {
		n: i64,
		count_star: bool,
	},
	Sum {
		accumulator: Value,
		n: u64,
		compensation: f64,
		seen_negative: bool,
	},
	Avg {
		sum: Value,
		n: i64,
		compensation: f64,
		seen_negative: bool,
	},
	Min(Multiset<Value>),
	Max(Multiset<Value>),
	MinSealed(SealingMin<WindowSlotKey, Value>),
	MaxSealed(SealingMax<WindowSlotKey, Value>),
	First(SealingEndpoint<WindowSlotKey, Value>),
	Last(SealingEndpoint<WindowSlotKey, Value>),
}

fn endpoint(grace: Duration) -> SealingEndpoint<WindowSlotKey, Value> {
	if grace.is_zero() {
		SealingEndpoint::default()
	} else {
		SealingEndpoint::with_grace(grace)
	}
}

impl AggregateSlot {
	fn empty(kind: SlotKind, grace: Duration) -> Self {
		match kind {
			SlotKind::Count {
				count_star,
			} => AggregateSlot::Count {
				n: 0,
				count_star,
			},
			SlotKind::Sum => AggregateSlot::Sum {
				accumulator: Value::none(),
				n: 0,
				compensation: 0.0,
				seen_negative: false,
			},
			SlotKind::Avg => AggregateSlot::Avg {
				sum: Value::none(),
				n: 0,
				compensation: 0.0,
				seen_negative: false,
			},
			SlotKind::Min => {
				if grace.is_zero() {
					AggregateSlot::Min(Multiset::default())
				} else {
					AggregateSlot::MinSealed(SealingMin::with_grace(grace))
				}
			}
			SlotKind::Max => {
				if grace.is_zero() {
					AggregateSlot::Max(Multiset::default())
				} else {
					AggregateSlot::MaxSealed(SealingMax::with_grace(grace))
				}
			}
			SlotKind::First => AggregateSlot::First(endpoint(grace)),
			SlotKind::Last => AggregateSlot::Last(endpoint(grace)),
		}
	}

	fn add(&mut self, coord: WindowSlotKey, input: &Option<Value>) {
		match self {
			AggregateSlot::Count {
				n,
				count_star,
			} => {
				if *count_star || present(input).is_some() {
					*n += 1;
				}
			}
			AggregateSlot::Sum {
				accumulator,
				n,
				compensation,
				seen_negative,
			} => {
				if let Some(v) = present(input) {
					if is_negative(v) {
						*seen_negative = true;
					}
					*accumulator = if *n == 0 {
						*compensation = 0.0;
						widen(v)
					} else {
						accumulate(accumulator, compensation, v, false)
					};
					*n += 1;
				}
			}
			AggregateSlot::Avg {
				sum,
				n,
				compensation,
				seen_negative,
			} => {
				if let Some(v) = present(input) {
					if is_negative(v) {
						*seen_negative = true;
					}
					*sum = if *n == 0 {
						*compensation = 0.0;
						widen(v)
					} else {
						accumulate(sum, compensation, v, false)
					};
					*n += 1;
				}
			}
			AggregateSlot::Min(set) | AggregateSlot::Max(set) => {
				if let Some(v) = present(input) {
					set.add(v.clone());
				}
			}
			AggregateSlot::MinSealed(s) => {
				if let Some(v) = present(input) {
					s.add(&(coord, v.clone()));
				}
			}
			AggregateSlot::MaxSealed(s) => {
				if let Some(v) = present(input) {
					s.add(&(coord, v.clone()));
				}
			}
			AggregateSlot::First(e) | AggregateSlot::Last(e) => {
				if let Some(v) = present(input) {
					e.add(&(coord, v.clone()));
				}
			}
		}
	}

	fn remove(&mut self, coord: WindowSlotKey, input: &Option<Value>) {
		match self {
			AggregateSlot::Count {
				n,
				count_star,
			} => {
				if *count_star || present(input).is_some() {
					*n -= 1;
				}
			}
			AggregateSlot::Sum {
				accumulator,
				n,
				compensation,
				..
			} => {
				if let Some(v) = present(input) {
					*n -= 1;
					*accumulator = if *n == 0 {
						*compensation = 0.0;
						Value::none()
					} else {
						accumulate(accumulator, compensation, v, true)
					};
				}
			}
			AggregateSlot::Avg {
				sum,
				n,
				compensation,
				..
			} => {
				if let Some(v) = present(input) {
					*n -= 1;
					*sum = if *n == 0 {
						*compensation = 0.0;
						Value::none()
					} else {
						accumulate(sum, compensation, v, true)
					};
				}
			}
			AggregateSlot::Min(set) | AggregateSlot::Max(set) => {
				if let Some(v) = present(input) {
					set.remove(v);
				}
			}
			AggregateSlot::MinSealed(s) => {
				if let Some(v) = present(input) {
					s.remove(&(coord, v.clone()));
				}
			}
			AggregateSlot::MaxSealed(s) => {
				if let Some(v) = present(input) {
					s.remove(&(coord, v.clone()));
				}
			}
			AggregateSlot::First(e) | AggregateSlot::Last(e) => {
				if let Some(v) = present(input) {
					e.remove(&(coord, v.clone()));
				}
			}
		}
	}

	fn merge(&mut self, other: &AggregateSlot) {
		match (self, other) {
			(
				AggregateSlot::Count {
					n,
					..
				},
				AggregateSlot::Count {
					n: on,
					..
				},
			) => *n += *on,
			(
				AggregateSlot::Sum {
					accumulator,
					n,
					compensation,
					seen_negative,
				},
				AggregateSlot::Sum {
					accumulator: other_accumulator,
					n: on,
					compensation: other_compensation,
					seen_negative: other_seen_negative,
				},
			) => {
				if *on > 0 {
					*seen_negative |= *other_seen_negative;
					if *n == 0 {
						*accumulator = other_accumulator.clone();
						*compensation = *other_compensation;
					} else {
						*accumulator = accumulate_pair(
							accumulator,
							compensation,
							other_accumulator,
							*other_compensation,
							false,
						);
					}
					*n += *on;
				}
			}
			(
				AggregateSlot::Avg {
					sum,
					n,
					compensation,
					seen_negative,
				},
				AggregateSlot::Avg {
					sum: osum,
					n: on,
					compensation: other_compensation,
					seen_negative: other_seen_negative,
				},
			) => {
				if *on > 0 {
					*seen_negative |= *other_seen_negative;
					if *n == 0 {
						*sum = osum.clone();
						*compensation = *other_compensation;
					} else {
						*sum = accumulate_pair(
							sum,
							compensation,
							osum,
							*other_compensation,
							false,
						);
					}
					*n += *on;
				}
			}
			(
				AggregateSlot::Min(set) | AggregateSlot::Max(set),
				AggregateSlot::Min(oset) | AggregateSlot::Max(oset),
			) => set.merge(oset),
			(AggregateSlot::MinSealed(a), AggregateSlot::MinSealed(b)) => a.absorb(b),
			(AggregateSlot::MaxSealed(a), AggregateSlot::MaxSealed(b)) => a.absorb(b),
			(
				AggregateSlot::First(a) | AggregateSlot::Last(a),
				AggregateSlot::First(b) | AggregateSlot::Last(b),
			) => a.absorb(b),
			_ => {}
		}
	}

	fn unmerge(&mut self, other: &AggregateSlot) {
		match (self, other) {
			(
				AggregateSlot::Count {
					n,
					..
				},
				AggregateSlot::Count {
					n: on,
					..
				},
			) => *n = (*n - *on).max(0),
			(
				AggregateSlot::Sum {
					accumulator,
					n,
					compensation,
					seen_negative,
				},
				AggregateSlot::Sum {
					accumulator: other_accumulator,
					n: on,
					compensation: other_compensation,
					seen_negative: other_seen_negative,
				},
			) => {
				if *on > 0 {
					*seen_negative |= *other_seen_negative;
					*n = n.saturating_sub(*on);
					if *n == 0 {
						*accumulator = Value::none();
						*compensation = 0.0;
					} else {
						*accumulator = accumulate_pair(
							accumulator,
							compensation,
							other_accumulator,
							*other_compensation,
							true,
						);
					}
				}
			}
			(
				AggregateSlot::Avg {
					sum,
					n,
					compensation,
					seen_negative,
				},
				AggregateSlot::Avg {
					sum: osum,
					n: on,
					compensation: other_compensation,
					seen_negative: other_seen_negative,
				},
			) => {
				if *on > 0 {
					*seen_negative |= *other_seen_negative;
					*n = (*n - *on).max(0);
					if *n == 0 {
						*sum = Value::none();
						*compensation = 0.0;
					} else {
						*sum = accumulate_pair(
							sum,
							compensation,
							osum,
							*other_compensation,
							true,
						);
					}
				}
			}
			(
				AggregateSlot::Min(set) | AggregateSlot::Max(set),
				AggregateSlot::Min(oset) | AggregateSlot::Max(oset),
			) => set.unmerge(oset),
			_ => {
				#[cfg(reifydb_assertions)]
				panic!("unmerge on non-invertible aggregate slot");
			}
		}
	}

	fn finalize(&self) -> Value {
		match self {
			AggregateSlot::Count {
				n,
				..
			} => Value::Int8(*n),
			AggregateSlot::Sum {
				accumulator,
				compensation,
				seen_negative,
				..
			} => finalize_compensated(accumulator, *compensation, *seen_negative),
			AggregateSlot::Avg {
				sum,
				n,
				compensation,
				seen_negative,
			} => finalize_compensated(sum, *compensation, *seen_negative)
				.checked_div(&Value::Int8(*n))
				.unwrap_or_else(Value::none),
			AggregateSlot::Min(set) => set.min().cloned().unwrap_or_else(Value::none),
			AggregateSlot::Max(set) => set.max().cloned().unwrap_or_else(Value::none),
			AggregateSlot::MinSealed(s) => s.min().unwrap_or_else(Value::none),
			AggregateSlot::MaxSealed(s) => s.max().unwrap_or_else(Value::none),
			AggregateSlot::First(e) => e.open().cloned().unwrap_or_else(Value::none),
			AggregateSlot::Last(e) => e.close().cloned().unwrap_or_else(Value::none),
		}
	}

	fn is_empty(&self) -> bool {
		match self {
			AggregateSlot::Count {
				n,
				..
			} => *n == 0,
			AggregateSlot::Sum {
				n,
				..
			} => *n == 0,
			AggregateSlot::Avg {
				n,
				..
			} => *n == 0,
			AggregateSlot::Min(set) | AggregateSlot::Max(set) => set.is_empty(),
			AggregateSlot::MinSealed(s) => s.is_empty(),
			AggregateSlot::MaxSealed(s) => s.is_empty(),
			AggregateSlot::First(e) | AggregateSlot::Last(e) => e.is_empty(),
		}
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RowAccumulator {
	slots: Vec<AggregateSlot>,
}

impl HeapSize for RowAccumulator {
	fn heap_size(&self) -> usize {
		self.slots.capacity() * mem::size_of::<AggregateSlot>()
	}
}

impl RowAccumulator {
	pub fn new(kinds: &[SlotKind], grace: Duration) -> Self {
		Self {
			slots: kinds.iter().map(|k| AggregateSlot::empty(*k, grace)).collect(),
		}
	}

	pub fn merge(&mut self, other: &RowAccumulator) {
		for (slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
			slot.merge(other_slot);
		}
	}

	pub fn unmerge(&mut self, other: &RowAccumulator) {
		for (slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
			slot.unmerge(other_slot);
		}
	}

	pub fn invertible(kinds: &[SlotKind], grace: Duration) -> bool {
		kinds.iter().all(|kind| match kind {
			SlotKind::Count {
				..
			}
			| SlotKind::Sum
			| SlotKind::Avg => true,
			SlotKind::Min | SlotKind::Max => grace.is_zero(),
			SlotKind::First | SlotKind::Last => false,
		})
	}
}

impl WindowAccumulator for RowAccumulator {
	type Contribution = (WindowSlotKey, Vec<Option<Value>>);
	type Output = Vec<Value>;

	fn add(&mut self, contribution: &Self::Contribution) {
		let (coord, values) = contribution;
		reifydb_assertions! {
			assert!(
				values.len() == self.slots.len(),
				"RowAccumulator contribution length {} != slot count {}; the zip below truncates to the \
				 shorter side, so a default-constructed zero-slot accumulator (e.g. routed through an engine \
				 that builds empties via Default instead of new(kinds)) would silently swallow every \
				 contribution",
				values.len(),
				self.slots.len()
			);
		}
		for (slot, input) in self.slots.iter_mut().zip(values.iter()) {
			slot.add(*coord, input);
		}
	}

	fn remove(&mut self, contribution: &Self::Contribution) {
		let (coord, values) = contribution;
		reifydb_assertions! {
			assert!(
				values.len() == self.slots.len(),
				"RowAccumulator contribution length {} != slot count {}; the zip below truncates to the \
				 shorter side, so a default-constructed zero-slot accumulator (e.g. routed through an engine \
				 that builds empties via Default instead of new(kinds)) would silently swallow every \
				 retraction",
				values.len(),
				self.slots.len()
			);
		}
		for (slot, input) in self.slots.iter_mut().zip(values.iter()) {
			slot.remove(*coord, input);
		}
	}

	fn finalize(&self) -> Option<Self::Output> {
		if self.is_empty() {
			return None;
		}
		Some(self.slots.iter().map(AggregateSlot::finalize).collect())
	}

	fn is_empty(&self) -> bool {
		self.slots.iter().all(AggregateSlot::is_empty)
	}

	fn merge(&mut self, other: &Self) {
		RowAccumulator::merge(self, other);
	}

	fn unmerge(&mut self, other: &Self) {
		RowAccumulator::unmerge(self, other);
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StampedAccumulator {
	inner: RowAccumulator,
	ts: u64,
}

impl HeapSize for StampedAccumulator {
	fn heap_size(&self) -> usize {
		self.inner.heap_size()
	}
}

impl StampedAccumulator {
	pub fn new(kinds: &[SlotKind], grace: Duration) -> Self {
		Self {
			inner: RowAccumulator::new(kinds, grace),
			ts: 0,
		}
	}

	pub fn inner(&self) -> &RowAccumulator {
		&self.inner
	}
}

impl WindowAccumulator for StampedAccumulator {
	type Contribution = ((WindowSlotKey, Vec<Option<Value>>), u64);
	type Output = Vec<Value>;

	fn add(&mut self, contribution: &Self::Contribution) {
		self.inner.add(&contribution.0);
		self.ts = self.ts.max(contribution.1);
	}

	fn remove(&mut self, contribution: &Self::Contribution) {
		self.inner.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<Self::Output> {
		self.inner.finalize()
	}

	fn is_empty(&self) -> bool {
		self.inner.is_empty()
	}

	fn stamp(&self) -> Option<u64> {
		if self.inner.is_empty() {
			None
		} else {
			Some(self.ts)
		}
	}
}

fn present(input: &Option<Value>) -> Option<&Value> {
	match input {
		Some(v) if !matches!(v, Value::None { .. }) => Some(v),
		_ => None,
	}
}

fn widen(v: &Value) -> Value {
	v.checked_add(v).and_then(|two| two.checked_sub(v)).unwrap_or_else(|| v.clone())
}

fn is_negative(v: &Value) -> bool {
	match v {
		Value::Float8(f) => f.value() < 0.0,
		Value::Float4(f) => f.value() < 0.0,
		Value::Int1(i) => *i < 0,
		Value::Int2(i) => *i < 0,
		Value::Int4(i) => *i < 0,
		Value::Int8(i) => *i < 0,
		Value::Int16(i) => *i < 0,
		_ => false,
	}
}

fn neumaier(sum: f64, compensation: &mut f64, x: f64) -> f64 {
	let t = sum + x;
	if sum.abs() >= x.abs() {
		*compensation += (sum - t) + x;
	} else {
		*compensation += (x - t) + sum;
	}
	t
}

fn accumulate(accumulator: &Value, compensation: &mut f64, v: &Value, negate: bool) -> Value {
	if let (Value::Float8(sum), Value::Float8(x)) = (accumulator, v) {
		let x = if negate {
			-x.value()
		} else {
			x.value()
		};
		Value::float8(neumaier(sum.value(), compensation, x))
	} else if negate {
		accumulator.checked_sub(v).unwrap_or_else(Value::none)
	} else {
		accumulator.checked_add(v).unwrap_or_else(Value::none)
	}
}

fn accumulate_pair(
	accumulator: &Value,
	compensation: &mut f64,
	other: &Value,
	other_compensation: f64,
	negate: bool,
) -> Value {
	let folded = accumulate(accumulator, compensation, other, negate);
	if let Value::Float8(sum) = &folded {
		let x = if negate {
			-other_compensation
		} else {
			other_compensation
		};
		Value::float8(neumaier(sum.value(), compensation, x))
	} else {
		folded
	}
}

fn finalize_compensated(accumulator: &Value, compensation: f64, seen_negative: bool) -> Value {
	match accumulator {
		Value::Float8(f) => {
			let x = f.value() + compensation;
			if !seen_negative && x < 0.0 {
				Value::float8(0.0)
			} else {
				Value::float8(x)
			}
		}
		other => other.clone(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn i4(v: i32) -> Option<Value> {
		Some(Value::Int4(v))
	}

	fn accumulator(kinds: &[SlotKind]) -> RowAccumulator {
		RowAccumulator::new(kinds, Duration::default())
	}

	fn at(seq: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::default(), seq)
	}

	fn coord(secs: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::from_timestamp(secs as i64).unwrap(), secs)
	}

	fn add(a: &mut RowAccumulator, seq: u64, values: Vec<Option<Value>>) {
		a.add(&(at(seq), values));
	}

	fn remove(a: &mut RowAccumulator, seq: u64, values: Vec<Option<Value>>) {
		a.remove(&(at(seq), values));
	}

	#[test]
	fn count_counts_rows_and_resets_on_empty() {
		let mut a = accumulator(&[SlotKind::Count {
			count_star: true,
		}]);
		assert!(a.is_empty());
		add(&mut a, 0, vec![None]);
		add(&mut a, 1, vec![None]);
		assert_eq!(a.finalize(), Some(vec![Value::Int8(2)]));
		remove(&mut a, 0, vec![None]);
		remove(&mut a, 1, vec![None]);
		assert!(a.is_empty());
		assert_eq!(a.finalize(), None);
	}

	#[test]
	fn count_col_ignores_none() {
		let mut a = accumulator(&[SlotKind::Count {
			count_star: false,
		}]);
		add(&mut a, 0, vec![i4(5)]);
		add(&mut a, 1, vec![Some(Value::none())]); // none -> not counted
		add(&mut a, 2, vec![i4(7)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int8(2)]));
	}

	#[test]
	fn sum_has_stable_widened_type_and_inverts() {
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![i4(5)]);
		// single contribution is already widened to Int16
		assert_eq!(a.finalize(), Some(vec![Value::Int16(5)]));
		add(&mut a, 1, vec![i4(3)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int16(8)]));
		// retraction inverts exactly
		remove(&mut a, 1, vec![i4(3)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int16(5)]));
	}

	#[test]
	fn sum_skips_none() {
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![i4(10)]);
		add(&mut a, 1, vec![Some(Value::none())]);
		assert_eq!(a.finalize(), Some(vec![Value::Int16(10)]));
	}

	#[test]
	fn avg_is_decimal_and_inverts() {
		let mut a = accumulator(&[SlotKind::Avg]);
		add(&mut a, 0, vec![i4(2)]);
		add(&mut a, 1, vec![i4(3)]);
		// (2 + 3) / 2 = 2.5 as Decimal
		let got = a.finalize().unwrap();
		assert!(matches!(got[0], Value::Decimal(_)), "avg is Decimal, got {:?}", got[0]);
		let expected = Value::Int16(5).checked_div(&Value::Int8(2)).unwrap();
		assert_eq!(got[0], expected);
		remove(&mut a, 1, vec![i4(3)]);
		assert_eq!(a.finalize().unwrap()[0], Value::Int16(2).checked_div(&Value::Int8(1)).unwrap());
	}

	#[test]
	fn min_max_via_multiset_invert() {
		let mut a = accumulator(&[SlotKind::Min, SlotKind::Max]);
		for (seq, v) in [5, 8, 6].into_iter().enumerate() {
			add(&mut a, seq as u64, vec![i4(v), i4(v)]);
		}
		assert_eq!(a.finalize(), Some(vec![Value::Int4(5), Value::Int4(8)]));
		// remove the current min (5) -> min becomes 6, max stays 8
		remove(&mut a, 0, vec![i4(5), i4(5)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int4(6), Value::Int4(8)]));
	}

	#[test]
	fn multi_slot_row_add_remove_inverse() {
		let kinds = [
			SlotKind::Count {
				count_star: true,
			},
			SlotKind::Sum,
			SlotKind::Min,
		];
		let mut a = accumulator(&kinds);
		add(&mut a, 0, vec![None, i4(100), i4(100)]);
		let snap = a.finalize();
		add(&mut a, 1, vec![None, i4(40), i4(40)]);
		remove(&mut a, 1, vec![None, i4(40), i4(40)]);
		assert_eq!(a.finalize(), snap, "add then remove restores all slots");
	}

	#[test]
	fn merge_equals_accumulating_all_into_one() {
		let kinds = [
			SlotKind::Count {
				count_star: true,
			},
			SlotKind::Sum,
			SlotKind::Avg,
			SlotKind::Min,
			SlotKind::Max,
		];
		// One accumulator holding every contribution directly.
		let mut whole = accumulator(&kinds);
		let rows = [(10, 10, 10), (40, 40, 40), (7, 7, 7), (99, 99, 99)];
		for (seq, (s, mn, mx)) in rows.into_iter().enumerate() {
			add(&mut whole, seq as u64, vec![None, i4(s), i4(s), i4(mn), i4(mx)]);
		}
		// Two partial accumulators (disjoint slots, as a rolling buffer would hold) merged.
		let mut left = accumulator(&kinds);
		for (seq, (s, mn, mx)) in rows[..2].iter().enumerate() {
			add(&mut left, seq as u64, vec![None, i4(*s), i4(*s), i4(*mn), i4(*mx)]);
		}
		let mut right = accumulator(&kinds);
		for (seq, (s, mn, mx)) in rows[2..].iter().enumerate() {
			add(&mut right, (seq + 2) as u64, vec![None, i4(*s), i4(*s), i4(*mn), i4(*mx)]);
		}
		left.merge(&right);
		assert_eq!(
			left.finalize(),
			whole.finalize(),
			"merge of two partials must equal one combined accumulator"
		);
	}

	#[test]
	fn merge_into_empty_takes_other_widened_sum() {
		let kinds = [SlotKind::Sum];
		let mut empty = accumulator(&kinds);
		let mut other = accumulator(&kinds);
		add(&mut other, 0, vec![i4(5)]);
		empty.merge(&other);
		// Empty-self merge must adopt the other's already-widened Int16, not stay none.
		assert_eq!(empty.finalize(), Some(vec![Value::Int16(5)]));
	}

	#[test]
	fn empty_when_all_removed() {
		let mut a = accumulator(&[SlotKind::Sum, SlotKind::Min]);
		add(&mut a, 0, vec![i4(1), i4(1)]);
		remove(&mut a, 0, vec![i4(1), i4(1)]);
		assert!(a.is_empty());
		assert_eq!(a.finalize(), None);
	}

	#[test]
	fn first_last_track_endpoints_by_coordinate() {
		// first/last order by the event coordinate; out-of-order arrival must still
		// yield the earliest/latest by coordinate, not by arrival.
		let mut a = RowAccumulator::new(&[SlotKind::First, SlotKind::Last], Duration::default());
		a.add(&(coord(20), vec![i4(20), i4(20)]));
		a.add(&(coord(10), vec![i4(10), i4(10)]));
		a.add(&(coord(30), vec![i4(30), i4(30)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(10), Value::Int4(30)]));
	}

	#[test]
	fn lateness_seals_aged_min_max_and_drops_late_retraction() {
		// With grace = 5s, an entry whose coordinate is more than 5s behind the
		// high-water mark is folded into the sealed scalar. The max stays correct, but a
		// retraction of that aged entry is a no-op (it is no longer in the live tail) -
		// this is the documented memory-vs-exactness trade, identical to chaindex.
		let grace = Duration::from_seconds(5).unwrap();
		let mut a = RowAccumulator::new(&[SlotKind::Max], grace);
		a.add(&(coord(0), vec![i4(100)])); // becomes sealed once high-water passes 5s
		a.add(&(coord(10), vec![i4(50)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(100)]), "sealed max still dominates");
		// Retracting the sealed entry cannot lower the max: it was already folded away.
		a.remove(&(coord(0), vec![i4(100)]));
		assert_eq!(
			a.finalize(),
			Some(vec![Value::Int4(100)]),
			"retraction older than grace is a no-op, so the sealed max survives"
		);
		// A retraction still inside the grace window does take effect.
		a.add(&(coord(12), vec![i4(70)]));
		a.remove(&(coord(12), vec![i4(70)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(100)]));
	}

	#[test]
	fn lateness_none_min_max_is_exact_under_retraction() {
		// Without grace, Min/Max use the exact Multiset and a retraction of any prior
		// value is honored regardless of age.
		let mut a = accumulator(&[SlotKind::Max]);
		add(&mut a, 0, vec![i4(100)]);
		add(&mut a, 1, vec![i4(50)]);
		remove(&mut a, 0, vec![i4(100)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int4(50)]), "exact path retracts the old max");
	}

	#[test]
	fn sealed_merge_matches_one_combined_accumulator() {
		// Rolling merges sub-accumulators; a sealed Min/Max/endpoint merge must equal one
		// accumulator that saw all contributions.
		let grace = Duration::from_seconds(60).unwrap();
		let kinds = [SlotKind::Min, SlotKind::Max, SlotKind::First, SlotKind::Last];
		let rows = [(5, 30), (8, 10), (3, 50), (12, 20)];
		let mut whole = RowAccumulator::new(&kinds, grace);
		for (i, (v, _)) in rows.iter().enumerate() {
			whole.add(&(coord((i as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		let mut left = RowAccumulator::new(&kinds, grace);
		for (i, (v, _)) in rows[..2].iter().enumerate() {
			left.add(&(coord((i as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		let mut right = RowAccumulator::new(&kinds, grace);
		for (i, (v, _)) in rows[2..].iter().enumerate() {
			right.add(&(coord(((i + 2) as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		left.merge(&right);
		assert_eq!(left.finalize(), whole.finalize(), "sealed merge must equal one combined accumulator");
	}

	#[test]
	fn finalize_clamps_negative_dust_to_exact_zero_for_nonnegative_data() {
		// Compensation bounds drift to ~1 ulp but cannot make it zero, so a
		// nonnegative sum can still land at -1e-13 after enough churn. The clamp
		// is the structural guarantee that volume-like data never publishes a
		// negative: any negative float result with no negative contribution ever
		// seen must finalize as exactly 0.0.
		assert_eq!(
			finalize_compensated(&Value::float8(-1e-13f64), 0.0, false),
			Value::float8(0.0f64),
			"negative dust with all-nonnegative history must clamp to exact 0"
		);
		assert_eq!(
			finalize_compensated(&Value::float8(-1e-13f64), 0.0, true),
			Value::float8(-1e-13f64),
			"seen_negative must disable the clamp"
		);
		assert_eq!(
			finalize_compensated(&Value::float8(1.0f64), -2.0, false),
			Value::float8(0.0f64),
			"the compensation term participates in the sign check"
		);
	}

	#[test]
	fn nonnegative_churn_never_finalizes_negative() {
		// End-to-end form of the clamp guarantee: a long deterministic
		// add/remove sequence of nonnegative dollar amounts must never observe a
		// negative finalized sum at any point, regardless of how the rounding
		// dust falls.
		let mut a = accumulator(&[SlotKind::Sum]);
		let mut pending: Vec<(u64, f64)> = Vec::new();
		let mut state = 0x9E37_79B9_7F4A_7C15u64;
		for round in 0..2_000u64 {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			let dollars = ((state >> 16) % 1_000_000_000) as f64 / 100.0;
			a.add(&(at(round), vec![Some(Value::float8(dollars))]));
			pending.push((round, dollars));
			if round % 2 == 1 {
				let (old_seq, old_dollars) = pending.remove(0);
				a.remove(&(at(old_seq), vec![Some(Value::float8(old_dollars))]));
			}
			if let Some(out) = a.finalize() {
				let Value::Float8(got) = &out[0] else {
					panic!("sum of Float8 must stay Float8, got {:?}", out[0]);
				};
				assert!(
					got.value() >= 0.0,
					"nonnegative sum finalized negative ({}) at round {round}",
					got.value()
				);
			}
		}
	}

	#[test]
	fn seen_negative_disables_the_zero_clamp() {
		// A legitimately negative sum (negative contributions were seen) must
		// pass through unclamped - the clamp is domain knowledge for
		// all-nonnegative data only, not a general floor.
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![Some(Value::float8(3.0f64))]);
		add(&mut a, 1, vec![Some(Value::float8(-5.0f64))]);
		let out = a.finalize().expect("two contributions");
		assert_eq!(out, vec![Value::float8(-2.0f64)], "genuinely negative sums must not be clamped");
	}

	#[test]
	fn kahan_compensation_preserves_small_terms_across_cancellation() {
		// The classic cancellation case the running accumulator hits when a huge
		// trade expires: naive f64 loses 3.14 when it is added to 1e16 (the low
		// bits round away), so after the 1e16 expires a naive sum returns 4.0 or
		// 0.0. The Neumaier compensation must carry the small term exactly.
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![Some(Value::float8(1e16f64))]);
		add(&mut a, 1, vec![Some(Value::float8(3.14f64))]);
		remove(&mut a, 0, vec![Some(Value::float8(1e16f64))]);
		let out = a.finalize().expect("one contribution remains");
		assert_eq!(out, vec![Value::float8(3.14f64)], "compensation must preserve the small term exactly");
	}

	#[test]
	fn sum_returns_none_after_float_churn_empties_it() {
		// n is an exact integer, so when every contribution is retracted the sum
		// must report the missing-value none regardless of accumulated float
		// dust (the n == 0 reset also clears the compensation term).
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![Some(Value::float8(1e16f64))]);
		add(&mut a, 1, vec![Some(Value::float8(3.14f64))]);
		remove(&mut a, 0, vec![Some(Value::float8(1e16f64))]);
		remove(&mut a, 1, vec![Some(Value::float8(3.14f64))]);
		assert!(a.is_empty());
		assert_eq!(a.finalize(), None, "an emptied sum must be none, not float dust");
	}

	#[test]
	fn kahan_sum_tracks_an_exact_cents_oracle_through_mixed_magnitude_churn() {
		// Rolling volume sums add and retract dollar amounts of wildly different
		// magnitudes for hours; the accepted-drift design relies on compensated
		// arithmetic keeping the error near one ulp instead of a growing random
		// walk. Drive a long deterministic add/remove sequence and compare
		// against an exact integer-cents oracle.
		let mut a = accumulator(&[SlotKind::Sum]);
		let mut oracle_cents: i128 = 0;
		let mut seq = 0u64;
		let mut pending: Vec<(u64, i64)> = Vec::new();
		let mut state = 0x243F_6A88_85A3_08D3u64;
		for round in 0..5_000u64 {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			let cents = ((state >> 16) % 1_000_000_000) as i64 + 1;
			let dollars = cents as f64 / 100.0;
			add(&mut a, seq, vec![Some(Value::float8(dollars))]);
			oracle_cents += cents as i128;
			pending.push((seq, cents));
			seq += 1;
			if round % 3 == 2 {
				let (old_seq, old_cents) = pending.remove(0);
				let old_dollars = old_cents as f64 / 100.0;
				remove(&mut a, old_seq, vec![Some(Value::float8(old_dollars))]);
				oracle_cents -= old_cents as i128;
			}
		}
		let out = a.finalize().expect("pending contributions remain");
		let Value::Float8(got) = &out[0] else {
			panic!("sum of Float8 must stay Float8, got {:?}", out[0]);
		};
		let expected = oracle_cents as f64 / 100.0;
		let tolerance = expected.abs() * 1e-12;
		assert!(
			(got.value() - expected).abs() <= tolerance,
			"compensated sum {} drifted from exact oracle {} by more than {}",
			got.value(),
			expected,
			tolerance
		);
	}

	#[test]
	fn unmerge_inverts_merge_for_all_invertible_slot_kinds() {
		// The runnable rolling engine maintains its per-group running
		// accumulator via merge(new coord state) / unmerge(expired coord state);
		// unmerge must be merge's exact inverse for every invertible kind or the
		// running output silently diverges from the buffer recombine.
		let kinds = [
			SlotKind::Count {
				count_star: true,
			},
			SlotKind::Sum,
			SlotKind::Avg,
			SlotKind::Min,
			SlotKind::Max,
		];
		let mut base = accumulator(&kinds);
		add(&mut base, 0, vec![i4(10), i4(10), i4(10), i4(10), i4(10)]);
		add(&mut base, 1, vec![i4(4), i4(4), i4(4), i4(4), i4(4)]);
		let snapshot = base.finalize();

		let mut other = accumulator(&kinds);
		add(&mut other, 2, vec![i4(7), i4(7), i4(7), i4(7), i4(7)]);
		add(&mut other, 3, vec![i4(1), i4(1), i4(1), i4(1), i4(1)]);

		let mut all_in_one = accumulator(&kinds);
		add(&mut all_in_one, 0, vec![i4(10), i4(10), i4(10), i4(10), i4(10)]);
		add(&mut all_in_one, 1, vec![i4(4), i4(4), i4(4), i4(4), i4(4)]);
		add(&mut all_in_one, 2, vec![i4(7), i4(7), i4(7), i4(7), i4(7)]);
		add(&mut all_in_one, 3, vec![i4(1), i4(1), i4(1), i4(1), i4(1)]);

		base.merge(&other);
		assert_eq!(
			base.finalize(),
			all_in_one.finalize(),
			"merge must be indistinguishable from accumulating everything into one"
		);
		base.unmerge(&other);
		assert_eq!(base.finalize(), snapshot, "unmerge must restore the pre-merge state exactly");
	}

	#[test]
	fn unmerge_to_empty_resets_sum_exactly() {
		// When the last coord of a group expires, unmerge drives n to zero; the
		// slot must reset to the exact none/zero state, not retain float dust.
		let kinds = [SlotKind::Sum];
		let mut running = accumulator(&kinds);
		let mut coord_state = accumulator(&kinds);
		add(&mut coord_state, 0, vec![Some(Value::float8(0.1f64))]);
		add(&mut coord_state, 1, vec![Some(Value::float8(0.2f64))]);
		running.merge(&coord_state);
		running.unmerge(&coord_state);
		assert!(running.is_empty(), "unmerging the only coord must empty the running accumulator");
		assert_eq!(running.finalize(), None);
	}

	#[test]
	fn invertible_gate_matches_slot_capabilities() {
		// The sub-flow wiring uses this predicate to decide runnable vs legacy
		// engine; a wrong answer either loses the optimization or runs unmerge
		// on kinds that cannot support it.
		let count = SlotKind::Count {
			count_star: true,
		};
		let zero = Duration::default();
		assert!(RowAccumulator::invertible(&[count, SlotKind::Sum, SlotKind::Avg], zero));
		assert!(RowAccumulator::invertible(&[SlotKind::Min, SlotKind::Max], zero));
		assert!(
			!RowAccumulator::invertible(&[SlotKind::Min], Duration::from_seconds(60).unwrap()),
			"grace turns Min/Max into sealed slots, which cannot unmerge"
		);
		assert!(!RowAccumulator::invertible(&[SlotKind::Sum, SlotKind::First], zero));
		assert!(!RowAccumulator::invertible(&[SlotKind::Last], zero));
	}
}
