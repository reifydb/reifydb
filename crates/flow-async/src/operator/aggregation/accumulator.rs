// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_rql::flow::aggregate::SlotKind;
use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		digest::Digest,
		duration::Duration,
		number::safe::{add::SafeAdd, div::SafeDiv, sub::SafeSub},
	},
};

use crate::{
	operator::state::sealing::{endpoint::SealingEndpoint, max::SealingMax, min::SealingMin},
	window::{
		accumulator::{
			MergeAccumulator, UnmergeAccumulator, WindowAccumulator, invertible::multiset::Multiset,
		},
		span::Slot,
	},
};

#[operator_state]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
		self.timestamp
	}

	fn from_order_key(coord: DateTime) -> Self {
		WindowSlotKey {
			timestamp: coord,
			seq: 0,
		}
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
	Span {
		n: i64,
	},
	Digest(Box<DigestSlot>),
}

#[operator_state]
#[derive(Clone, Debug)]
pub struct DigestSlot {
	accuracy: Option<u32>,
	digest: Option<Digest>,
}

impl DigestSlot {
	fn add(&mut self, value: &Value) {
		match (self.accuracy, value) {
			(None, Value::Digest(part)) => self.merge_digest(part),
			(Some(accuracy), value) => {
				let digest = self.digest.get_or_insert_with(|| {
					Digest::new(value.get_type(), accuracy).unwrap_or_else(|err| {
						panic!(
							"digest slot cannot start from {value}, which passed input checks: {err}"
						)
					})
				});
				digest.add_value(value).unwrap_or_else(|err| {
					panic!("digest slot add of {value} failed after input checks: {err}")
				});
			}
			(None, value) => panic!("digest slot without an accuracy takes only digests, got {value}"),
		}
		self.reset_when_empty();
	}

	fn remove(&mut self, value: &Value) {
		match (self.accuracy, value) {
			(None, Value::Digest(part)) => self.unmerge_digest(part),
			(Some(_), value) => {
				let Some(digest) = self.digest.as_mut() else {
					panic!("digest slot remove of {value} from an empty slot");
				};
				digest.remove_value(value).unwrap_or_else(|err| {
					panic!("digest slot remove of {value} failed after input checks: {err}")
				});
			}
			(None, value) => panic!("digest slot without an accuracy takes only digests, got {value}"),
		}
		self.reset_when_empty();
	}

	fn merge(&mut self, other: &DigestSlot) {
		if let Some(part) = &other.digest {
			self.merge_digest(part);
			self.reset_when_empty();
		}
	}

	fn unmerge(&mut self, other: &DigestSlot) {
		if let Some(part) = &other.digest {
			self.unmerge_digest(part);
			self.reset_when_empty();
		}
	}

	fn merge_digest(&mut self, part: &Digest) {
		match &mut self.digest {
			Some(digest) => {
				digest.merge(part).unwrap_or_else(|err| panic!("digest slot merge failed: {err}"))
			}
			None => self.digest = Some(part.clone()),
		}
	}

	fn unmerge_digest(&mut self, part: &Digest) {
		match &mut self.digest {
			Some(digest) => {
				digest.unmerge(part).unwrap_or_else(|err| panic!("digest slot unmerge failed: {err}"))
			}
			None if part.count() == 0 => {}
			None => panic!("digest slot unmerge of {} values from an empty slot", part.count()),
		}
	}

	fn reset_when_empty(&mut self) {
		if self.digest.as_ref().is_some_and(|digest| digest.bucket_count() == 0 && digest.count() == 0) {
			self.digest = None;
		}
	}

	fn finalize(&self) -> Value {
		match &self.digest {
			Some(digest) => Value::Digest(Box::new(digest.clone())),
			None => Value::none(),
		}
	}

	fn is_empty(&self) -> bool {
		self.digest.is_none()
	}
}

fn endpoint(immutable: Option<Duration>) -> SealingEndpoint<WindowSlotKey, Value> {
	match immutable {
		Some(immutable) => SealingEndpoint::immutable(immutable),
		None => SealingEndpoint::default(),
	}
}

impl AggregateSlot {
	fn empty(kind: SlotKind, immutable: Option<Duration>) -> Self {
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
			SlotKind::Min => match immutable {
				Some(immutable) => AggregateSlot::MinSealed(SealingMin::immutable(immutable)),
				None => AggregateSlot::Min(Multiset::default()),
			},
			SlotKind::Max | SlotKind::WindowLast => match immutable {
				Some(immutable) => AggregateSlot::MaxSealed(SealingMax::immutable(immutable)),
				None => AggregateSlot::Max(Multiset::default()),
			},
			SlotKind::First => AggregateSlot::First(endpoint(immutable)),
			SlotKind::Last => AggregateSlot::Last(endpoint(immutable)),
			SlotKind::WindowStart | SlotKind::WindowEnd | SlotKind::WindowDuration => AggregateSlot::Span {
				n: 0,
			},
			SlotKind::Digest {
				accuracy,
			} => AggregateSlot::Digest(Box::new(DigestSlot {
				accuracy,
				digest: None,
			})),
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
			AggregateSlot::Span {
				n,
			} => *n += 1,
			AggregateSlot::Digest(slot) => {
				if let Some(v) = present(input) {
					slot.add(v);
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
					if *n == 0 {
						#[cfg(reifydb_assertions)]
						panic!("Sum remove from an empty slot");
						#[cfg(not(reifydb_assertions))]
						return;
					}
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
			AggregateSlot::Span {
				n,
			} => *n -= 1,
			AggregateSlot::Digest(slot) => {
				if let Some(v) = present(input) {
					slot.remove(v);
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
			) if *on > 0 => {
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
			) if *on > 0 => {
				*seen_negative |= *other_seen_negative;
				if *n == 0 {
					*sum = osum.clone();
					*compensation = *other_compensation;
				} else {
					*sum = accumulate_pair(sum, compensation, osum, *other_compensation, false);
				}
				*n += *on;
			}
			(
				AggregateSlot::Min(set) | AggregateSlot::Max(set),
				AggregateSlot::Min(oset) | AggregateSlot::Max(oset),
			) => set.merge(oset),
			(AggregateSlot::MinSealed(a), AggregateSlot::MinSealed(b)) => a.merge(b),
			(AggregateSlot::MaxSealed(a), AggregateSlot::MaxSealed(b)) => a.merge(b),
			(
				AggregateSlot::First(a) | AggregateSlot::Last(a),
				AggregateSlot::First(b) | AggregateSlot::Last(b),
			) => a.merge(b),
			(
				AggregateSlot::Span {
					n,
				},
				AggregateSlot::Span {
					n: on,
				},
			) => *n += *on,
			(AggregateSlot::Digest(a), AggregateSlot::Digest(b)) => a.merge(b),
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
			(
				AggregateSlot::Span {
					n,
				},
				AggregateSlot::Span {
					n: on,
				},
			) => *n = (*n - *on).max(0),
			(AggregateSlot::Digest(a), AggregateSlot::Digest(b)) => a.unmerge(b),
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
			AggregateSlot::Span {
				..
			} => Value::none(),
			AggregateSlot::Digest(slot) => slot.finalize(),
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
			AggregateSlot::Span {
				n,
			} => *n == 0,
			AggregateSlot::Digest(slot) => slot.is_empty(),
		}
	}
}

#[operator_state]
#[derive(Clone, Debug, Default)]
pub struct RowAccumulator {
	slots: Vec<AggregateSlot>,
	rows: u64,
}

impl HeapSize for RowAccumulator {
	fn heap_size(&self) -> usize {
		self.slots.capacity() * mem::size_of::<AggregateSlot>()
	}
}

impl RowAccumulator {
	pub fn new(kinds: &[SlotKind], immutable: Option<Duration>) -> Self {
		Self {
			slots: kinds.iter().map(|k| AggregateSlot::empty(*k, immutable)).collect(),
			rows: 0,
		}
	}

	pub fn merge(&mut self, other: &RowAccumulator) {
		for (slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
			slot.merge(other_slot);
		}
		self.rows += other.rows;
	}

	pub fn unmerge(&mut self, other: &RowAccumulator) {
		for (slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
			slot.unmerge(other_slot);
		}
		self.rows = self.rows.checked_sub(other.rows).unwrap_or_else(|| {
			panic!("RowAccumulator unmerge of {} rows from {} rows", other.rows, self.rows)
		});
	}

	pub fn invertible(kinds: &[SlotKind], immutable: Option<Duration>) -> bool {
		kinds.iter().all(|kind| match kind {
			SlotKind::Count {
				..
			}
			| SlotKind::Sum
			| SlotKind::Avg => true,
			SlotKind::Min | SlotKind::Max | SlotKind::WindowLast => immutable.is_none(),
			SlotKind::WindowStart | SlotKind::WindowEnd | SlotKind::WindowDuration => true,
			SlotKind::First | SlotKind::Last => false,
			SlotKind::Digest {
				..
			} => true,
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
		self.rows += 1;
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
		self.rows = self.rows.checked_sub(1).expect("RowAccumulator remove of a row it never added");
	}

	fn finalize(&self) -> Option<Self::Output> {
		if self.is_empty() {
			return None;
		}
		Some(self.slots.iter().map(AggregateSlot::finalize).collect())
	}

	fn is_empty(&self) -> bool {
		self.rows == 0 && self.slots.iter().all(AggregateSlot::is_empty)
	}
}

impl MergeAccumulator for RowAccumulator {
	fn merge(&mut self, other: &Self) {
		RowAccumulator::merge(self, other);
	}
}

impl UnmergeAccumulator for RowAccumulator {
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
	use reifydb_codec::row::operator::state::{OperatorState, decode};
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::{
		operator::state::seal::coord::Coord,
		window::{
			accumulator::testkit::{Op, drive},
			span::WindowSpan,
		},
	};

	fn i4(v: i32) -> Option<Value> {
		Some(Value::Int4(v))
	}

	#[test]
	fn window_slot_key_order_key_survives_storage_and_ignores_seq() {
		// A stored key that orders differently from the writer makes the sweep reclaim live groups.
		let key = WindowSlotKey {
			timestamp: DateTime::from_nanos(1_700_000_000_123_456_789),
			seq: 7,
		};
		let bytes = key.encode_state().unwrap();
		let restored = decode::<WindowSlotKey>(&bytes).unwrap();

		assert_eq!(restored.order_key(), key.order_key());
		assert_eq!(
			restored.order_key().to_order(),
			key.timestamp.to_order(),
			"the order key is the stored layout, so sub-millisecond detail must survive it"
		);

		let same_millis_other_seq = WindowSlotKey {
			timestamp: key.timestamp,
			seq: 99,
		};
		let other_bytes = same_millis_other_seq.encode_state().unwrap();
		assert_eq!(
			decode::<WindowSlotKey>(&other_bytes).unwrap().order_key(),
			restored.order_key(),
			"seq must not leak into the order key"
		);
	}

	#[test]
	fn two_events_in_one_window_share_an_anchor_whatever_their_seq() {
		// A seq that survives the bucketing arithmetic gives every event its own window start, so
		// one logical window shatters into one window per event - each with its own accumulator
		// and its own emitted row, silently rather than as a failure.
		let duration = Duration::from_seconds(60).expect("representable span");
		let base = 1_700_000_040_000i64;

		let early = WindowSlotKey::new(DateTime::from_epoch_millis(base).expect("representable"), 0);
		let late = WindowSlotKey::new(
			DateTime::from_epoch_millis(base + 59_999).expect("representable"),
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
					DateTime::from_epoch_millis(base + 60_000).expect("representable"),
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

		let bytes = acc.encode_state().unwrap();
		let restored = decode::<RowAccumulator>(&bytes).unwrap();

		assert_eq!(restored.finalize(), acc.finalize());
		assert_eq!(
			restored.finalize().unwrap(),
			vec![Value::Int8(3), Value::Int16(17), Value::Int4(3), Value::Int4(5)]
		);
	}

	fn accumulator(kinds: &[SlotKind]) -> RowAccumulator {
		RowAccumulator::new(kinds, None)
	}

	fn at_time(secs: u64, seq: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::from_epoch_secs(secs as i64).unwrap(), seq)
	}

	fn dt(secs: u64) -> Value {
		Value::DateTime(DateTime::from_epoch_secs(secs as i64).unwrap())
	}

	#[test]
	fn window_last_reports_the_newest_event_time_not_the_newest_arrival() {
		// Rows reach a window out of order: the 30s trade arrives first, the 20s trade last. The answer to
		// "when did this bucket last see a trade" is 30s. Ordering by arrival sequence instead of by event
		// time would answer 20s, and every late-arriving row would silently rewind the reported time.
		let mut a = accumulator(&[SlotKind::WindowLast]);
		a.add(&(at_time(30, 1), vec![Some(dt(30))]));
		a.add(&(at_time(10, 2), vec![Some(dt(10))]));
		a.add(&(at_time(20, 3), vec![Some(dt(20))]));
		assert_eq!(a.finalize(), Some(vec![dt(30)]));
	}

	#[test]
	fn removing_the_newest_row_moves_window_last_back_to_the_runner_up() {
		// A window recomputes by retracting rows. Holding a running maximum that never retracts would leave
		// the reported time pinned to a trade that is no longer in the bucket.
		let mut a = accumulator(&[SlotKind::WindowLast]);
		a.add(&(at_time(10, 1), vec![Some(dt(10))]));
		a.add(&(at_time(30, 2), vec![Some(dt(30))]));
		a.remove(&(at_time(30, 2), vec![Some(dt(30))]));
		assert_eq!(a.finalize(), Some(vec![dt(10)]));
	}

	#[test]
	fn window_last_stores_one_entry_per_event_time_not_one_per_row() {
		// An entry per row grows the saved window with its row count, and every batch decodes and re-encodes
		// all of it.
		let mut one = accumulator(&[SlotKind::WindowLast]);
		one.add(&(at_time(30, 0), vec![Some(dt(30))]));
		let mut many = accumulator(&[SlotKind::WindowLast]);
		for seq in 0..1000 {
			many.add(&(at_time(30, seq), vec![Some(dt(30))]));
		}
		let one_bytes = one.encode_state().unwrap().as_slice().len();
		let many_bytes = many.encode_state().unwrap().as_slice().len();
		assert!(
			many_bytes <= one_bytes + 8,
			"1000 rows at one event time encode to {many_bytes} bytes, one row to {one_bytes}"
		);
		assert_eq!(many.finalize(), Some(vec![dt(30)]));

		// Rows at one event time must still retract one by one, or the time outlives its last row.
		for seq in 0..999 {
			many.remove(&(at_time(30, seq), vec![Some(dt(30))]));
		}
		assert_eq!(many.finalize(), Some(vec![dt(30)]), "one row at 30s is still in the window");
		many.remove(&(at_time(30, 999), vec![Some(dt(30))]));
		assert!(many.is_empty(), "every row at 30s was retracted");
	}

	#[test]
	fn a_span_slot_counts_its_rows_so_a_lone_boundary_window_still_emits() {
		// A boundary is a property of the window, not of its rows, so the slot keeps no row value - it is
		// overwritten from the span at emit. But an accumulator that reports empty is dropped rather than
		// emitted, so a window whose only output is a boundary would silently produce nothing. The slot
		// must still count the rows that reached it, and stop being populated once they are all retracted.
		let mut a = accumulator(&[SlotKind::WindowStart]);
		assert!(a.is_empty());
		a.add(&(at_time(10, 1), vec![Some(dt(10))]));
		assert!(!a.is_empty(), "a window holding a row must not report empty");
		assert_eq!(a.finalize(), Some(vec![Value::none()]), "the value is filled from the span at emit");
		a.remove(&(at_time(10, 1), vec![Some(dt(10))]));
		assert!(a.is_empty());
	}

	#[test]
	fn a_span_slot_does_not_make_a_populated_window_look_empty() {
		// An empty accumulator is dropped rather than emitted. A span slot reports empty because it holds
		// nothing, so it must not drag a window that really has rows down with it.
		let mut a = accumulator(&[
			SlotKind::Count {
				count_star: true,
			},
			SlotKind::WindowStart,
		]);
		a.add(&(at_time(10, 1), vec![None, None]));
		assert!(!a.is_empty());
		assert_eq!(a.finalize().unwrap()[0], Value::Int8(1));
	}

	#[test]
	fn merging_two_accumulators_keeps_the_later_window_last() {
		// Windows merge partial accumulators from separate batches. Taking the merge target's value rather
		// than the later of the two would report a stale time whenever the newer batch merged in second.
		let mut a = accumulator(&[SlotKind::WindowLast]);
		a.add(&(at_time(10, 1), vec![Some(dt(10))]));
		let mut b = accumulator(&[SlotKind::WindowLast]);
		b.add(&(at_time(40, 2), vec![Some(dt(40))]));
		a.merge(&b);
		assert_eq!(a.finalize(), Some(vec![dt(40)]));
	}

	#[test]
	fn a_span_slot_is_invertible_and_window_last_is_invertible_only_while_unsealed() {
		// invertible() lets a rolling window unmerge expired rows; claiming it for a slot that cannot unmerge
		// leaves a stale value.
		assert!(RowAccumulator::invertible(&[SlotKind::WindowStart], None));
		assert!(RowAccumulator::invertible(&[SlotKind::WindowEnd], None));
		assert!(RowAccumulator::invertible(&[SlotKind::WindowDuration], None));
		assert!(
			RowAccumulator::invertible(&[SlotKind::WindowLast], None),
			"an unsealed window last counts each event time, so it unmerges like max"
		);
		assert!(
			!RowAccumulator::invertible(&[SlotKind::WindowLast], Some(Duration::from_seconds(60).unwrap())),
			"a sealed window last folds old times into one value and cannot give them back"
		);
	}

	#[test]
	fn unmerging_expired_slots_from_window_last_matches_a_rebuild_from_the_slots_left() {
		// A rolling window subtracts expired slots from a running total; a wrong unmerge reports a time whose
		// rows already left.
		let rows: [&[(u64, u64)]; 4] =
			[&[(10, 1), (30, 2)], &[(30, 3), (30, 4)], &[(20, 5)], &[(30, 6), (25, 7)]];
		let slots: Vec<RowAccumulator> = rows
			.iter()
			.map(|slot| {
				let mut a = accumulator(&[SlotKind::WindowLast]);
				for &(secs, seq) in *slot {
					a.add(&(at_time(secs, seq), vec![Some(dt(secs))]));
				}
				a
			})
			.collect();
		let mut running = accumulator(&[SlotKind::WindowLast]);
		for slot in &slots {
			running.merge(slot);
		}

		let mut left: Vec<usize> = (0..slots.len()).collect();
		for expired in [1, 0, 3, 2] {
			running.unmerge(&slots[expired]);
			left.retain(|&index| index != expired);
			let mut rebuilt = accumulator(&[SlotKind::WindowLast]);
			for &index in &left {
				rebuilt.merge(&slots[index]);
			}
			assert_eq!(
				running.finalize(),
				rebuilt.finalize(),
				"after expiring slot {expired}, slots {left:?} remain"
			);
		}
		assert!(running.is_empty(), "every slot expired");
	}

	fn at(seq: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::default(), seq)
	}

	fn coord(secs: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::from_epoch_secs(secs as i64).unwrap(), secs)
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
		let mut a = RowAccumulator::new(&[SlotKind::First, SlotKind::Last], None);
		a.add(&(coord(20), vec![i4(20), i4(20)]));
		a.add(&(coord(10), vec![i4(10), i4(10)]));
		a.add(&(coord(30), vec![i4(30), i4(30)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(10), Value::Int4(30)]));
	}

	#[test]
	fn an_immutable_span_seals_aged_min_max_and_drops_late_retraction() {
		// An entry more than one immutable span behind the high-water mark is folded into the sealed
		// scalar, so retracting it is a no-op: the deliberate memory-vs-exactness trade.
		let immutable = Duration::from_seconds(5).unwrap();
		let mut a = RowAccumulator::new(&[SlotKind::Max], Some(immutable));
		a.add(&(coord(0), vec![i4(100)])); // becomes sealed once high-water passes 5s
		a.add(&(coord(10), vec![i4(50)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(100)]), "sealed max still dominates");
		// Retracting the sealed entry cannot lower the max: it was already folded away.
		a.remove(&(coord(0), vec![i4(100)]));
		assert_eq!(
			a.finalize(),
			Some(vec![Value::Int4(100)]),
			"retraction older than the immutable span is a no-op, so the sealed max survives"
		);
		// A retraction still inside the immutable window does take effect.
		a.add(&(coord(12), vec![i4(70)]));
		a.remove(&(coord(12), vec![i4(70)]));
		assert_eq!(a.finalize(), Some(vec![Value::Int4(100)]));
	}

	#[test]
	fn a_zero_immutable_span_keeps_min_max_exact_under_retraction() {
		// Without an immutable span, Min/Max use the exact Multiset and a retraction of any prior
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
		let immutable = Duration::from_seconds(60).unwrap();
		let kinds = [SlotKind::Min, SlotKind::Max, SlotKind::First, SlotKind::Last];
		let rows = [(5, 30), (8, 10), (3, 50), (12, 20)];
		let mut whole = RowAccumulator::new(&kinds, Some(immutable));
		for (i, (v, _)) in rows.iter().enumerate() {
			whole.add(&(coord((i as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		let mut left = RowAccumulator::new(&kinds, Some(immutable));
		for (i, (v, _)) in rows[..2].iter().enumerate() {
			left.add(&(coord((i as u64) * 10), vec![i4(*v), i4(*v), i4(*v), i4(*v)]));
		}
		let mut right = RowAccumulator::new(&kinds, Some(immutable));
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
	#[allow(clippy::approx_constant)]
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
	#[allow(clippy::approx_constant)]
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
		let mut pending: Vec<(u64, i64)> = Vec::new();
		let mut state = 0x243F_6A88_85A3_08D3u64;
		for (seq, round) in (0u64..).zip(0..5_000u64) {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			let cents = ((state >> 16) % 1_000_000_000) as i64 + 1;
			let dollars = cents as f64 / 100.0;
			add(&mut a, seq, vec![Some(Value::float8(dollars))]);
			oracle_cents += cents as i128;
			pending.push((seq, cents));
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
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "Sum remove from an empty slot")]
	fn sum_remove_from_an_empty_slot_is_a_named_assertion() {
		// A retraction with no matching add is a state bug, so it must fail with a name instead of an integer
		// overflow.
		let mut a = accumulator(&[SlotKind::Sum]);
		remove(&mut a, 0, vec![i4(5)]);
	}

	#[test]
	fn invertible_gate_matches_slot_capabilities() {
		// This predicate picks the engine, so a wrong answer either loses the optimization or
		// runs unmerge on kinds that cannot support it.
		let count = SlotKind::Count {
			count_star: true,
		};
		assert!(RowAccumulator::invertible(&[count, SlotKind::Sum, SlotKind::Avg], None));
		assert!(RowAccumulator::invertible(&[SlotKind::Min, SlotKind::Max], None));
		assert!(
			!RowAccumulator::invertible(&[SlotKind::Min], Some(Duration::from_seconds(60).unwrap())),
			"an immutable span turns Min/Max into sealed slots, which cannot unmerge"
		);
		assert!(!RowAccumulator::invertible(&[SlotKind::Sum, SlotKind::First], None));
		assert!(!RowAccumulator::invertible(&[SlotKind::Last], None));
	}

	const PPM: u32 = 10_000;

	fn digest_kind(accuracy: Option<u32>) -> SlotKind {
		SlotKind::Digest {
			accuracy,
		}
	}

	fn f8(v: f64) -> Option<Value> {
		Some(Value::float8(v))
	}

	fn oracle(inner: ValueType, accuracy: u32, values: &[Value]) -> Digest {
		let mut digest = Digest::new(inner, accuracy).unwrap();
		for value in values {
			digest.add_value(value).unwrap();
		}
		digest
	}

	fn digest_value(digest: Digest) -> Value {
		Value::Digest(Box::new(digest))
	}

	fn state_bytes(a: &RowAccumulator) -> Vec<u8> {
		a.encode_state().unwrap().as_slice().to_vec()
	}

	struct Lcg(u64);

	impl Lcg {
		fn next(&mut self) -> u64 {
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			self.0 >> 33
		}
	}

	#[test]
	fn a_digest_slot_is_none_until_a_value_and_add_then_remove_restores_the_empty_state() {
		// A slot that keeps an emptied digest encodes unlike a fresh one, so stored state drifts from a
		// rebuild.
		let fresh = accumulator(&[digest_kind(Some(PPM))]);
		assert_eq!(fresh.slots[0].finalize(), Value::none(), "a slot with no value must finalize none");
		let mut a = fresh.clone();
		let probe = (at(0), vec![f8(5.0)]);
		drive(&mut a, &[Op::Add(probe.clone())]);
		assert_eq!(
			a.finalize(),
			Some(vec![digest_value(oracle(ValueType::Float8, PPM, &[Value::float8(5.0)]))])
		);
		drive(&mut a, &[Op::Remove(probe)]);
		assert!(a.is_empty());
		assert_eq!(a.finalize(), None);
		assert_eq!(
			state_bytes(&a),
			state_bytes(&fresh),
			"add then remove must encode exactly as the empty slot"
		);
	}

	#[test]
	fn a_digest_slot_skips_none_and_equals_an_oracle_through_seeded_add_remove_churn() {
		// A dropped retraction or a counted none shifts every rank, so the answer drifts from the rows present.
		let mut a = accumulator(&[digest_kind(Some(PPM))]);
		let mut live: Vec<(u64, f64)> = Vec::new();
		#[allow(clippy::unusual_byte_groupings)]
		let mut rng = Lcg(0x5EED_D16E_57u64);
		for seq in 0..3_000u64 {
			let roll = rng.next();
			if roll.is_multiple_of(5) {
				a.add(&(at(seq), vec![Some(Value::none())]));
				continue;
			}
			if roll.is_multiple_of(3) && !live.is_empty() {
				let (old_seq, old) = live.swap_remove((rng.next() % live.len() as u64) as usize);
				a.remove(&(at(old_seq), vec![f8(old)]));
			} else {
				let v = match rng.next() % 4 {
					0 => 0.0,
					1 => -((rng.next() % 1_000_000) as f64) / 7.0,
					_ => (rng.next() % 1_000_000) as f64 / 3.0,
				};
				a.add(&(at(seq), vec![f8(v)]));
				live.push((seq, v));
			}
			if seq % 97 == 0 {
				let values: Vec<Value> = live.iter().map(|(_, v)| Value::float8(*v)).collect();
				let expected = if values.is_empty() {
					Value::none()
				} else {
					digest_value(oracle(ValueType::Float8, PPM, &values))
				};
				assert_eq!(
					a.slots[0].finalize(),
					expected,
					"digest diverged from the live rows at step {seq}"
				);
			}
		}
	}

	#[test]
	fn unmerging_a_digest_slot_restores_the_running_state_and_merge_equals_one_accumulator() {
		// Rolling keeps a running digest by merge and unmerge, so a lossy inverse reports rows that left the
		// frame.
		let kinds = [digest_kind(Some(PPM)), SlotKind::Sum];
		let mut base = accumulator(&kinds);
		let mut other = accumulator(&kinds);
		let mut whole = accumulator(&kinds);
		for (seq, v) in [(0, 1.5), (1, -2.0), (2, 0.0)] {
			base.add(&(at(seq), vec![f8(v), f8(v)]));
			whole.add(&(at(seq), vec![f8(v), f8(v)]));
		}
		for (seq, v) in [(3, 900.0), (4, 1.5)] {
			other.add(&(at(seq), vec![f8(v), f8(v)]));
			whole.add(&(at(seq), vec![f8(v), f8(v)]));
		}
		let snapshot = state_bytes(&base);
		base.merge(&other);
		assert_eq!(base.finalize(), whole.finalize(), "merge must equal accumulating every row into one");
		base.unmerge(&other);
		assert_eq!(state_bytes(&base), snapshot, "unmerge must restore the pre-merge state byte for byte");

		let lone_kinds = [digest_kind(Some(PPM))];
		let mut lone = accumulator(&lone_kinds);
		for (seq, v) in [(0, 1.5), (1, -2.0), (2, 0.0)] {
			lone.add(&(at(seq), vec![f8(v)]));
		}
		let mut running = accumulator(&lone_kinds);
		running.merge(&lone);
		running.unmerge(&lone);
		assert_eq!(
			state_bytes(&running),
			state_bytes(&accumulator(&lone_kinds)),
			"unmerging the only part must empty it"
		);
	}

	#[test]
	fn a_digest_input_merges_on_add_and_unmerges_on_remove() {
		// A digest input added as one value instead of merged counts each stored digest once, not its rows.
		let first = oracle(ValueType::Int4, PPM, &[Value::Int4(1), Value::Int4(2), Value::Int4(3)]);
		let second = oracle(ValueType::Int4, PPM, &[Value::Int4(10), Value::Int4(2_000)]);
		let fresh = accumulator(&[digest_kind(None)]);
		let mut a = fresh.clone();
		a.add(&(at(0), vec![Some(digest_value(first.clone()))]));
		a.add(&(at(1), vec![Some(digest_value(second.clone()))]));
		let all = [1, 2, 3, 10, 2_000].map(Value::Int4);
		assert_eq!(a.finalize(), Some(vec![digest_value(oracle(ValueType::Int4, PPM, &all))]));
		a.remove(&(at(0), vec![Some(digest_value(first))]));
		assert_eq!(a.finalize(), Some(vec![digest_value(second.clone())]));
		a.remove(&(at(1), vec![Some(digest_value(second))]));
		assert_eq!(
			state_bytes(&a),
			state_bytes(&fresh),
			"removing every merged digest must return the empty slot"
		);
	}

	#[test]
	#[should_panic(expected = "cannot merge digest(Float8, 10000 ppm) with digest(Float8, 50000 ppm)")]
	fn merging_digests_of_different_accuracy_in_one_slot_is_a_named_failure() {
		// Summing counts of buckets with different widths gives answers that match neither accuracy.
		let mut a = accumulator(&[digest_kind(None)]);
		a.add(&(at(0), vec![Some(digest_value(oracle(ValueType::Float8, PPM, &[Value::float8(1.0)])))]));
		a.add(&(at(1), vec![Some(digest_value(oracle(ValueType::Float8, 50_000, &[Value::float8(1.0)])))]));
	}

	#[test]
	fn a_duration_digest_slot_keeps_its_duration_inner_type_through_remove() {
		// A slot that loses its inner type reads a latency percentile back as a bare float instead of a
		// duration.
		let values: Vec<Value> = [3, 7_200, 25 * 3_600]
			.iter()
			.map(|s| Value::Duration(Duration::from_seconds(*s).unwrap()))
			.collect();
		let mut a = accumulator(&[digest_kind(Some(1_000))]);
		for (seq, value) in values.iter().enumerate() {
			a.add(&(at(seq as u64), vec![Some(value.clone())]));
		}
		a.remove(&(at(0), vec![Some(values[0].clone())]));
		let out = a.finalize().expect("two durations remain");
		let Value::Digest(digest) = &out[0] else {
			panic!("a digest slot must finalize a digest, got {:?}", out[0]);
		};
		assert_eq!(digest.inner(), &ValueType::Duration);
		assert_eq!(**digest, oracle(ValueType::Duration, 1_000, &values[1..]));
	}

	#[test]
	fn a_digest_slot_is_invertible_with_or_without_an_immutable_span() {
		// A digest keeps no per-row history, so sealing cannot fold it; refusing the running path costs every
		// frame.
		let immutable = Some(Duration::from_seconds(60).unwrap());
		assert!(RowAccumulator::invertible(&[digest_kind(Some(PPM))], None));
		assert!(RowAccumulator::invertible(&[digest_kind(None)], immutable));
		assert!(!RowAccumulator::invertible(&[digest_kind(Some(PPM)), SlotKind::First], None));
		assert!(!RowAccumulator::invertible(&[digest_kind(Some(PPM)), SlotKind::Max], immutable));
	}

	#[test]
	fn a_row_of_only_none_values_keeps_its_group_until_the_row_is_retracted() {
		// Batch returns an all-none group with sum none and count 0; a flow that drops it disagrees with the
		// query.
		let kinds = [
			SlotKind::Sum,
			SlotKind::Count {
				count_star: false,
			},
			digest_kind(Some(PPM)),
		];
		let mut a = accumulator(&kinds);
		let row = (at(0), vec![Some(Value::none()), Some(Value::none()), Some(Value::none())]);
		a.add(&row);
		assert!(!a.is_empty(), "a group holding a row must not report empty");
		assert_eq!(a.finalize(), Some(vec![Value::none(), Value::Int8(0), Value::none()]));
		let mut running = accumulator(&kinds);
		running.merge(&a);
		assert_eq!(running.finalize(), a.finalize(), "a merged all-none part must keep the running group");
		running.unmerge(&a);
		assert!(running.is_empty(), "unmerging the only all-none part must empty the running group");
		a.remove(&row);
		assert!(a.is_empty(), "a group whose rows were all retracted must disappear");
		assert_eq!(a.finalize(), None);
	}

	#[test]
	#[should_panic(expected = "RowAccumulator remove of a row it never added")]
	fn removing_a_row_that_was_never_added_is_a_named_failure() {
		// A retraction with no matching add is a state bug; wrapping the row count would resurrect the group.
		let mut a = accumulator(&[SlotKind::Count {
			count_star: true,
		}]);
		a.remove(&(at(0), vec![None]));
	}
}
