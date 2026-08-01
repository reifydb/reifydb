// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::metrics::heap::HeapSize;
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_flow::window::{
	accumulator::{
		WindowAccumulator,
		invertible::Multiset,
		sealing::{SealingEndpoint, SealingMax, SealingMin},
	},
	span::{Slot, WindowCoord},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		duration::Duration,
		number::safe::{add::SafeAdd, div::SafeDiv, sub::SafeSub},
	},
};
use rkyv::Archive;

#[operator_state]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[rkyv(derive(Hash, PartialEq, Eq, PartialOrd, Ord))]
pub struct WindowSlotKey {
	pub timestamp: DateTime,
	pub seq: u64,
}

impl HeapSize for WindowSlotKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl WindowSlotKey {
	pub fn new(timestamp: DateTime, seq: u64) -> Self {
		Self {
			timestamp,
			seq,
		}
	}
}

impl Slot for WindowSlotKey {
	type Coord = DateTime;

	fn order_key(&self) -> DateTime {
		<DateTime as WindowCoord>::from_order(self.timestamp.timestamp_millis() as u64)
	}

	fn from_order_key(coord: DateTime) -> Self {
		WindowSlotKey {
			timestamp: coord,
			seq: 0,
		}
	}

	fn archived_order_key(archived: &<Self as Archive>::Archived) -> DateTime {
		DateTime::from_timestamp_millis(archived.timestamp.timestamp_millis() as u64).unwrap_or_default()
	}
}

#[operator_state]
#[derive(Clone, Debug)]
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

#[operator_state]
#[derive(Clone, Debug, Default)]
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
	use reifydb_codec::state::OperatorState;
	use reifydb_flow::window::span::WindowSpan;

	use super::*;

	fn i4(v: i32) -> Option<Value> {
		Some(Value::Int4(v))
	}

	#[test]
	fn window_slot_key_archived_order_key_matches_the_owned_one() {
		// The order key deliberately ignores seq, and the archived read must agree with the owned
		// one or the meta sweep reclaims on a different ordering than the path that wrote it.
		let key = WindowSlotKey {
			timestamp: DateTime::from_nanos(1_700_000_000_123_456_789),
			seq: 7,
		};
		let bytes = key.encode_state(DateTime::EPOCH).unwrap();
		let archived = WindowSlotKey::archived(&bytes).unwrap();

		assert_eq!(WindowSlotKey::archived_order_key(archived), key.order_key());
		assert_eq!(
			WindowSlotKey::archived_order_key(archived).to_order(),
			1_700_000_000_123,
			"the order key is milliseconds; sub-millisecond detail must not reach it"
		);

		let same_millis_other_seq = WindowSlotKey {
			timestamp: key.timestamp,
			seq: 99,
		};
		let other_bytes = same_millis_other_seq.encode_state(DateTime::EPOCH).unwrap();
		assert_eq!(
			WindowSlotKey::archived_order_key(WindowSlotKey::archived(&other_bytes).unwrap()),
			WindowSlotKey::archived_order_key(archived),
			"seq must not leak into the order key"
		);
	}

	#[test]
	fn two_events_in_one_window_share_an_anchor_whatever_their_seq() {
		// A seq that survives the bucketing arithmetic gives every event its own window start, so
		// one logical window shatters into one window per event - each with its own accumulator
		// and its own emitted row, silently rather than as a failure.
		let duration = Duration::from_seconds(60).expect("representable span");
		let base = 1_700_000_040_000u64;

		let early = WindowSlotKey::new(DateTime::from_timestamp_millis(base).expect("representable"), 0);
		let late = WindowSlotKey::new(
			DateTime::from_timestamp_millis(base + 59_999).expect("representable"),
			u64::MAX,
		);

		let early_span = WindowSpan::for_coord(early.order_key(), duration);
		let late_span = WindowSpan::for_coord(late.order_key(), duration);

		assert_eq!(
			early_span, late_span,
			"events in the same minute must share one window regardless of seq or sub-window offset"
		);
		assert!(early_span.contains(early.order_key()));
		assert!(early_span.contains(late.order_key()));
		assert!(
			!early_span.contains(
				WindowSlotKey::new(
					DateTime::from_timestamp_millis(base + 60_000).expect("representable"),
					0,
				)
				.order_key()
			),
			"the next minute's first event must fall outside, or windows would overlap"
		);
	}

	#[test]
	fn test_row_accumulator_archived_round_trip() {
		// RowAccumulator is the memory-dominant persisted state type, so a round trip has to
		// reproduce finalize() exactly, not merely decode.
		let mut acc = accumulator(&[
			SlotKind::Count {
				count_star: false,
			},
			SlotKind::Sum,
			SlotKind::Min,
			SlotKind::First,
		]);
		add(&mut acc, 1, vec![i4(5), i4(5), i4(5), i4(5)]);
		add(&mut acc, 2, vec![i4(3), i4(3), i4(3), i4(3)]);
		add(&mut acc, 3, vec![i4(9), i4(9), i4(9), i4(9)]);

		let bytes = acc.encode_state(DateTime::from_nanos(42)).unwrap();
		let archived = RowAccumulator::archived(&bytes).unwrap();
		let restored = RowAccumulator::materialize(archived).unwrap();

		assert_eq!(restored.finalize(), acc.finalize());
		assert_eq!(
			restored.finalize().unwrap(),
			vec![Value::Int8(3), Value::Int16(17), Value::Int4(3), Value::Int4(5)]
		);
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
		let mut whole = accumulator(&kinds);
		let rows = [(10, 10, 10), (40, 40, 40), (7, 7, 7), (99, 99, 99)];
		for (seq, (s, mn, mx)) in rows.into_iter().enumerate() {
			add(&mut whole, seq as u64, vec![None, i4(s), i4(s), i4(mn), i4(mx)]);
		}
		// Two partials over disjoint slots, as a rolling buffer would hold them.
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
		// An entry more than one grace behind the high-water mark is folded into the sealed
		// scalar, so retracting it is a no-op: the deliberate memory-vs-exactness trade.
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
		// Compensation bounds drift to ~1 ulp but cannot make it zero, so a nonnegative sum can
		// still land at -1e-13; the clamp is what stops volume-like data publishing a negative.
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
		// End-to-end form of the clamp guarantee: no intermediate finalize may go negative,
		// however the rounding dust falls.
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
		// The clamp is domain knowledge for all-nonnegative data, not a general floor, so a
		// genuinely negative sum must pass through.
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![Some(Value::float8(3.0f64))]);
		add(&mut a, 1, vec![Some(Value::float8(-5.0f64))]);
		let out = a.finalize().expect("two contributions");
		assert_eq!(out, vec![Value::float8(-2.0f64)], "genuinely negative sums must not be clamped");
	}

	#[test]
	fn kahan_compensation_preserves_small_terms_across_cancellation() {
		// The cancellation the running accumulator hits when a huge trade expires: naive f64
		// rounds 3.14 away against 1e16, so retracting the 1e16 leaves 4.0 or 0.0.
		let mut a = accumulator(&[SlotKind::Sum]);
		add(&mut a, 0, vec![Some(Value::float8(1e16f64))]);
		add(&mut a, 1, vec![Some(Value::float8(3.14f64))]);
		remove(&mut a, 0, vec![Some(Value::float8(1e16f64))]);
		let out = a.finalize().expect("one contribution remains");
		assert_eq!(out, vec![Value::float8(3.14f64)], "compensation must preserve the small term exactly");
	}

	#[test]
	fn sum_returns_none_after_float_churn_empties_it() {
		// The contribution count is an exact integer, so a fully retracted sum must report none
		// regardless of accumulated float dust.
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
		// Rolling volume sums churn mixed magnitudes for hours, and the accepted-drift design
		// relies on the error staying near one ulp rather than becoming a random walk.
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
		// The rolling engine maintains its running accumulator by merging new coords and
		// unmerging expired ones, so any kind where unmerge is not merge's exact inverse
		// silently diverges from the buffer recombine.
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
		// When the last coord of a group expires the slot must reset to exactly none, not retain
		// float dust.
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
		// This predicate picks the engine, so a wrong answer either loses the optimization or
		// runs unmerge on kinds that cannot support it.
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
