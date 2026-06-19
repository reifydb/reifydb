// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::{Add, Rem, Sub};

use reifydb_core::window::{
	accumulator::{
		WindowAccumulator,
		invertible::Multiset,
		sealing::{SealingEndpoint, SealingMax, SealingMin},
	},
	span::Slot,
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
	},
	Avg {
		sum: Value,
		n: i64,
	},
	Min(Multiset<Value>),
	Max(Multiset<Value>),
	MinSealed(SealingMin<WindowSlotKey, Value>),
	MaxSealed(SealingMax<WindowSlotKey, Value>),
	First(SealingEndpoint<WindowSlotKey, Value>),
	Last(SealingEndpoint<WindowSlotKey, Value>),
}

fn endpoint(lateness: Option<Duration>) -> SealingEndpoint<WindowSlotKey, Value> {
	match lateness {
		Some(l) => SealingEndpoint::with_lateness(l),
		None => SealingEndpoint::default(),
	}
}

impl AggregateSlot {
	fn empty(kind: SlotKind, lateness: Option<Duration>) -> Self {
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
			},
			SlotKind::Avg => AggregateSlot::Avg {
				sum: Value::none(),
				n: 0,
			},
			SlotKind::Min => match lateness {
				Some(l) => AggregateSlot::MinSealed(SealingMin::with_lateness(l)),
				None => AggregateSlot::Min(Multiset::default()),
			},
			SlotKind::Max => match lateness {
				Some(l) => AggregateSlot::MaxSealed(SealingMax::with_lateness(l)),
				None => AggregateSlot::Max(Multiset::default()),
			},
			SlotKind::First => AggregateSlot::First(endpoint(lateness)),
			SlotKind::Last => AggregateSlot::Last(endpoint(lateness)),
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
			} => {
				if let Some(v) = present(input) {
					*accumulator = if *n == 0 {
						widen(v)
					} else {
						accumulator.checked_add(v).unwrap_or_else(Value::none)
					};
					*n += 1;
				}
			}
			AggregateSlot::Avg {
				sum,
				n,
			} => {
				if let Some(v) = present(input) {
					*sum = if *n == 0 {
						widen(v)
					} else {
						sum.checked_add(v).unwrap_or_else(Value::none)
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
			} => {
				if let Some(v) = present(input) {
					*n -= 1;
					*accumulator = if *n == 0 {
						Value::none()
					} else {
						accumulator.checked_sub(v).unwrap_or_else(Value::none)
					};
				}
			}
			AggregateSlot::Avg {
				sum,
				n,
			} => {
				if let Some(v) = present(input) {
					*n -= 1;
					*sum = if *n == 0 {
						Value::none()
					} else {
						sum.checked_sub(v).unwrap_or_else(Value::none)
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
				},
				AggregateSlot::Sum {
					accumulator: other_accumulator,
					n: on,
				},
			) => {
				if *on > 0 {
					*accumulator = if *n == 0 {
						other_accumulator.clone()
					} else {
						accumulator.checked_add(other_accumulator).unwrap_or_else(Value::none)
					};
					*n += *on;
				}
			}
			(
				AggregateSlot::Avg {
					sum,
					n,
				},
				AggregateSlot::Avg {
					sum: osum,
					n: on,
				},
			) => {
				if *on > 0 {
					*sum = if *n == 0 {
						osum.clone()
					} else {
						sum.checked_add(osum).unwrap_or_else(Value::none)
					};
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

	fn finalize(&self) -> Value {
		match self {
			AggregateSlot::Count {
				n,
				..
			} => Value::Int8(*n),
			AggregateSlot::Sum {
				accumulator,
				..
			} => accumulator.clone(),
			AggregateSlot::Avg {
				sum,
				n,
			} => sum.checked_div(&Value::Int8(*n)).unwrap_or_else(Value::none),
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

impl RowAccumulator {
	pub fn new(kinds: &[SlotKind], lateness: Option<Duration>) -> Self {
		Self {
			slots: kinds.iter().map(|k| AggregateSlot::empty(*k, lateness)).collect(),
		}
	}

	pub fn merge(&mut self, other: &RowAccumulator) {
		for (slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
			slot.merge(other_slot);
		}
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StampedAccumulator {
	inner: RowAccumulator,
	ts: u64,
}

impl StampedAccumulator {
	pub fn new(kinds: &[SlotKind], lateness: Option<Duration>) -> Self {
		Self {
			inner: RowAccumulator::new(kinds, lateness),
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

#[cfg(test)]
mod tests {
	use super::*;

	fn i4(v: i32) -> Option<Value> {
		Some(Value::Int4(v))
	}

	fn accumulator(kinds: &[SlotKind]) -> RowAccumulator {
		RowAccumulator::new(kinds, None)
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
		let mut a = RowAccumulator::new(&[SlotKind::First, SlotKind::Last], None);
		a.add(&(coord(20), vec![i4(20), i4(20)]));
		a.add(&(coord(10), vec![i4(10), i4(10)]));
		a.add(&(coord(30), vec![i4(30), i4(30)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(10), Value::Int4(30)]));
	}

	#[test]
	fn lateness_seals_aged_min_max_and_drops_late_retraction() {
		// With lateness = 5s, an entry whose coordinate is more than 5s behind the
		// high-water mark is folded into the sealed scalar. The max stays correct, but a
		// retraction of that aged entry is a no-op (it is no longer in the live tail) -
		// this is the documented memory-vs-exactness trade, identical to chaindex.
		let lateness = Duration::from_seconds(5).unwrap();
		let mut a = RowAccumulator::new(&[SlotKind::Max], Some(lateness));
		a.add(&(coord(0), vec![i4(100)])); // becomes sealed once high-water passes 5s
		a.add(&(coord(10), vec![i4(50)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(100)]), "sealed max still dominates");
		// Retracting the sealed entry cannot lower the max: it was already folded away.
		a.remove(&(coord(0), vec![i4(100)]));
		assert_eq!(
			a.finalize(),
			Some(vec![Value::Int4(100)]),
			"retraction older than lateness is a no-op, so the sealed max survives"
		);
		// A retraction still inside the lateness window does take effect.
		a.add(&(coord(12), vec![i4(70)]));
		a.remove(&(coord(12), vec![i4(70)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(100)]));
	}

	#[test]
	fn lateness_none_min_max_is_exact_under_retraction() {
		// Without lateness, Min/Max use the exact Multiset and a retraction of any prior
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
		let lateness = Duration::from_seconds(60).unwrap();
		let kinds = [SlotKind::Min, SlotKind::Max, SlotKind::First, SlotKind::Last];
		let rows = [(5, 30), (8, 10), (3, 50), (12, 20)];
		let mut whole = RowAccumulator::new(&kinds, Some(lateness));
		for (i, (v, _)) in rows.iter().enumerate() {
			whole.add(&(coord((i as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		let mut left = RowAccumulator::new(&kinds, Some(lateness));
		for (i, (v, _)) in rows[..2].iter().enumerate() {
			left.add(&(coord((i as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		let mut right = RowAccumulator::new(&kinds, Some(lateness));
		for (i, (v, _)) in rows[2..].iter().enumerate() {
			right.add(&(coord(((i + 2) as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		left.merge(&right);
		assert_eq!(left.finalize(), whole.finalize(), "sealed merge must equal one combined accumulator");
	}
}
