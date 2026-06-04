// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::window::accumulator::{WindowAccumulator, invertible::Multiset};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		number::safe::{add::SafeAdd, div::SafeDiv, sub::SafeSub},
	},
};
use serde::{Deserialize, Serialize};

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
}

impl AggregateSlot {
	fn empty(kind: SlotKind) -> Self {
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
			SlotKind::Min => AggregateSlot::Min(Multiset::default()),
			SlotKind::Max => AggregateSlot::Max(Multiset::default()),
		}
	}

	fn add(&mut self, input: &Option<Value>) {
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
		}
	}

	fn remove(&mut self, input: &Option<Value>) {
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
		}
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RowAccumulator {
	slots: Vec<AggregateSlot>,
}

impl RowAccumulator {
	pub fn new(kinds: &[SlotKind]) -> Self {
		Self {
			slots: kinds.iter().map(|k| AggregateSlot::empty(*k)).collect(),
		}
	}

	pub fn merge(&mut self, other: &RowAccumulator) {
		for (slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
			slot.merge(other_slot);
		}
	}
}

impl WindowAccumulator for RowAccumulator {
	type Contribution = Vec<Option<Value>>;
	type Output = Vec<Value>;

	fn add(&mut self, contribution: &Self::Contribution) {
		reifydb_assertions! {
			assert!(
				contribution.len() == self.slots.len(),
				"RowAccumulator contribution length {} != slot count {}; the zip below truncates to the \
				 shorter side, so a default-constructed zero-slot accumulator (e.g. routed through an engine \
				 that builds empties via Default instead of new(kinds)) would silently swallow every \
				 contribution",
				contribution.len(),
				self.slots.len()
			);
		}
		for (slot, input) in self.slots.iter_mut().zip(contribution.iter()) {
			slot.add(input);
		}
	}

	fn remove(&mut self, contribution: &Self::Contribution) {
		reifydb_assertions! {
			assert!(
				contribution.len() == self.slots.len(),
				"RowAccumulator contribution length {} != slot count {}; the zip below truncates to the \
				 shorter side, so a default-constructed zero-slot accumulator (e.g. routed through an engine \
				 that builds empties via Default instead of new(kinds)) would silently swallow every \
				 retraction",
				contribution.len(),
				self.slots.len()
			);
		}
		for (slot, input) in self.slots.iter_mut().zip(contribution.iter()) {
			slot.remove(input);
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
	pub fn new(kinds: &[SlotKind]) -> Self {
		Self {
			inner: RowAccumulator::new(kinds),
			ts: 0,
		}
	}

	pub fn inner(&self) -> &RowAccumulator {
		&self.inner
	}
}

impl WindowAccumulator for StampedAccumulator {
	type Contribution = (Vec<Option<Value>>, u64);
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
		RowAccumulator::new(kinds)
	}

	#[test]
	fn count_counts_rows_and_resets_on_empty() {
		let mut a = accumulator(&[SlotKind::Count {
			count_star: true,
		}]);
		assert!(a.is_empty());
		a.add(&vec![None]);
		a.add(&vec![None]);
		assert_eq!(a.finalize(), Some(vec![Value::Int8(2)]));
		a.remove(&vec![None]);
		a.remove(&vec![None]);
		assert!(a.is_empty());
		assert_eq!(a.finalize(), None);
	}

	#[test]
	fn count_col_ignores_none() {
		let mut a = accumulator(&[SlotKind::Count {
			count_star: false,
		}]);
		a.add(&vec![i4(5)]);
		a.add(&vec![Some(Value::none())]); // none -> not counted
		a.add(&vec![i4(7)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int8(2)]));
	}

	#[test]
	fn sum_has_stable_widened_type_and_inverts() {
		let mut a = accumulator(&[SlotKind::Sum]);
		a.add(&vec![i4(5)]);
		// single contribution is already widened to Int16
		assert_eq!(a.finalize(), Some(vec![Value::Int16(5)]));
		a.add(&vec![i4(3)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int16(8)]));
		// retraction inverts exactly
		a.remove(&vec![i4(3)]);
		assert_eq!(a.finalize(), Some(vec![Value::Int16(5)]));
	}

	#[test]
	fn sum_skips_none() {
		let mut a = accumulator(&[SlotKind::Sum]);
		a.add(&vec![i4(10)]);
		a.add(&vec![Some(Value::none())]);
		assert_eq!(a.finalize(), Some(vec![Value::Int16(10)]));
	}

	#[test]
	fn avg_is_decimal_and_inverts() {
		let mut a = accumulator(&[SlotKind::Avg]);
		a.add(&vec![i4(2)]);
		a.add(&vec![i4(3)]);
		// (2 + 3) / 2 = 2.5 as Decimal
		let got = a.finalize().unwrap();
		assert!(matches!(got[0], Value::Decimal(_)), "avg is Decimal, got {:?}", got[0]);
		let expected = Value::Int16(5).checked_div(&Value::Int8(2)).unwrap();
		assert_eq!(got[0], expected);
		a.remove(&vec![i4(3)]);
		assert_eq!(a.finalize().unwrap()[0], Value::Int16(2).checked_div(&Value::Int8(1)).unwrap());
	}

	#[test]
	fn min_max_via_multiset_invert() {
		let mut a = accumulator(&[SlotKind::Min, SlotKind::Max]);
		for v in [5, 8, 6] {
			a.add(&vec![i4(v), i4(v)]);
		}
		assert_eq!(a.finalize(), Some(vec![Value::Int4(5), Value::Int4(8)]));
		// remove the current min (5) -> min becomes 6, max stays 8
		a.remove(&vec![i4(5), i4(5)]);
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
		a.add(&vec![None, i4(100), i4(100)]);
		let snap = a.finalize();
		a.add(&vec![None, i4(40), i4(40)]);
		a.remove(&vec![None, i4(40), i4(40)]);
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
		for (s, mn, mx) in rows {
			whole.add(&vec![None, i4(s), i4(s), i4(mn), i4(mx)]);
		}
		// Two partial accumulators (disjoint slots, as a rolling buffer would hold) merged.
		let mut left = accumulator(&kinds);
		for (s, mn, mx) in &rows[..2] {
			left.add(&vec![None, i4(*s), i4(*s), i4(*mn), i4(*mx)]);
		}
		let mut right = accumulator(&kinds);
		for (s, mn, mx) in &rows[2..] {
			right.add(&vec![None, i4(*s), i4(*s), i4(*mn), i4(*mx)]);
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
		other.add(&vec![i4(5)]);
		empty.merge(&other);
		// Empty-self merge must adopt the other's already-widened Int16, not stay none.
		assert_eq!(empty.finalize(), Some(vec![Value::Int16(5)]));
	}

	#[test]
	fn empty_when_all_removed() {
		let mut a = accumulator(&[SlotKind::Sum, SlotKind::Min]);
		a.add(&vec![i4(1), i4(1)]);
		a.remove(&vec![i4(1), i4(1)]);
		assert!(a.is_empty());
		assert_eq!(a.finalize(), None);
	}
}
