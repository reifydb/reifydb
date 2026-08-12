// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::{EncodedOperatorRow, decode},
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	metrics::heap::HeapSize,
	state::store::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{timer::Timer, window::accumulator::WindowAccumulator};

/// One wheel mutation a shell issued, in issue order. Arm and disarm are distinct variants
/// because the pair is order-sensitive: a disarm landing after its arm cancels a live timer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordedTimer {
	Armed(Timer),
	Disarmed(Timer),
}

impl RecordedTimer {
	pub(crate) fn armed(at: DateTime, kind: TimerKind, key: EncodedKey) -> Self {
		Self::Armed(Timer {
			at,
			kind,
			key,
		})
	}

	pub(crate) fn disarmed(at: DateTime, kind: TimerKind, key: EncodedKey) -> Self {
		Self::Disarmed(Timer {
			at,
			kind,
			key,
		})
	}
}

#[derive(Default)]
pub(crate) struct MockStore {
	data: HashMap<Vec<u8>, EncodedOperatorRow>,
	groups: HashMap<Vec<u8>, GroupId>,
	rows: HashMap<(GroupId, Vec<u8>), u64>,
	next_row: u64,
	timers: Option<Vec<RecordedTimer>>,
	flow_watermark: Option<DateTime>,
}

impl MockStore {
	/// Opt in to recording wheel mutations. The default store still refuses them, so the
	/// engine suites keep proving that the engine itself never touches the wheel.
	pub(crate) fn recording_timers() -> Self {
		Self {
			timers: Some(Vec::new()),
			..Self::default()
		}
	}

	pub(crate) fn timers(&self) -> &[RecordedTimer] {
		self.timers.as_deref().unwrap_or_default()
	}

	fn record_timer(&mut self, recorded: RecordedTimer) -> Result<()> {
		let Some(timers) = self.timers.as_mut() else {
			unreachable!("the window engine never touches timers; only the shell above it does")
		};
		timers.push(recorded);
		Ok(())
	}

	fn keyspace_count(&self, keyspace: Keyspace) -> usize {
		self.data
			.keys()
			.filter(|k| OperatorStateKey::decode_inner(k).is_some_and(|(_, found, _)| found == keyspace))
			.count()
	}

	pub(crate) fn index_entry_count(&mut self) -> usize {
		self.keyspace_count(Keyspace::EXPIRY)
	}

	pub(crate) fn buffer_entry_count(&mut self) -> usize {
		self.keyspace_count(Keyspace::BUFFER)
	}

	pub(crate) fn buffer_coord_count<A: WindowAccumulator>(&mut self) -> usize {
		self.data
			.iter()
			.filter(|(k, _)| {
				OperatorStateKey::decode_inner(k).is_some_and(|(_, found, _)| found == Keyspace::BUFFER)
			})
			.map(|(_, bytes)| {
				decode::<BTreeMap<u64, A>>(bytes).expect("persisted window buffer must decode").len()
			})
			.sum()
	}

	pub(crate) fn running_entry_count(&mut self) -> usize {
		self.keyspace_count(Keyspace::RUNNING)
	}

	pub(crate) fn meta_entry_count(&mut self) -> usize {
		self.keyspace_count(Keyspace::WINDOW_META)
	}

	/// Simulates phase-1 group reclamation: the accumulators are erased while the
	/// due-ordered expiry index, which lives outside the group's range, is left behind.
	pub(crate) fn drop_accumulator_entries(&mut self) -> usize {
		let keys: Vec<Vec<u8>> = self
			.data
			.keys()
			.filter(|k| {
				OperatorStateKey::decode_inner(k)
					.is_some_and(|(_, found, _)| found == Keyspace::ACCUMULATOR)
			})
			.cloned()
			.collect();
		for key in &keys {
			self.data.remove(key);
		}
		keys.len()
	}

	/// The same phase, widened to every data keyspace a group can hold - the shape engines that
	/// keep no ACCUMULATOR see. The root group is spared, and the row-number mapping survives on
	/// top of that because it is an identity keyspace rather than a data one.
	pub(crate) fn drop_group_data_entries(&mut self) -> usize {
		let keys: Vec<Vec<u8>> = self
			.data
			.keys()
			.filter(|k| {
				OperatorStateKey::decode_inner(k)
					.is_some_and(|(group, found, _)| !group.is_root() && found.is_data())
			})
			.cloned()
			.collect();
		for key in &keys {
			self.data.remove(key);
		}
		keys.len()
	}

	pub(crate) fn mapping_entry_count(&mut self) -> usize {
		self.keyspace_count(Keyspace::ROW_NUMBER_MAPPING)
	}

	pub(crate) fn seed_mapping_key(&mut self, suffix: u8) {
		self.data.insert(
			OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::ROW_NUMBER_MAPPING, vec![suffix])
				.as_slice()
				.to_vec(),
			EncodedOperatorRow::new(&[0u8], DateTime::EPOCH),
		);
	}

	pub(crate) fn contains_row_mapping(&self, group: GroupId, key: &EncodedKey) -> bool {
		self.rows.contains_key(&(group, key.as_bytes().to_vec()))
	}
}

use reifydb_core::state::store::{TimerKind, TimerStore};

impl TimerStore for MockStore {
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record_timer(RecordedTimer::armed(at, kind, key.clone()))
	}

	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record_timer(RecordedTimer::disarmed(at, kind, key.clone()))
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.flow_watermark)
	}
}

impl StateStore for MockStore {
	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
		let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
		Ok(*self.groups.entry(group.as_bytes().to_vec()).or_insert(next))
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		Ok(self.groups.get(group.as_bytes()).copied())
	}

	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		Ok(self.data.get(key.as_slice()).cloned())
	}
	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		for key in keys {
			if let Some(b) = self.data.get(key.as_slice()) {
				visit(key.clone(), b.clone())?;
			}
		}
		Ok(())
	}
	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
		self.data.insert(key.as_slice().to_vec(), payload);
		Ok(())
	}
	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.data.remove(key.as_slice());
		Ok(())
	}
	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let after_start = |k: &[u8]| match &range.start {
			Bound::Included(s) => k >= s.as_bytes(),
			Bound::Excluded(s) => k > s.as_bytes(),
			Bound::Unbounded => true,
		};
		let before_end = |k: &[u8]| match &range.end {
			Bound::Included(e) => k <= e.as_bytes(),
			Bound::Excluded(e) => k < e.as_bytes(),
			Bound::Unbounded => true,
		};
		let mut matched: Vec<(Vec<u8>, EncodedOperatorRow)> = self
			.data
			.iter()
			.filter(|(k, _)| after_start(k) && before_end(k))
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();
		matched.sort_by(|a, b| a.0.cmp(&b.0));
		if let Some(limit) = limit {
			matched.truncate(limit);
		}
		for (k, b) in matched {
			let Some(k) = GroupStateKey::from_framed(EncodedKey::new(k)) else {
				continue;
			};
			visit(k, b)?;
		}
		Ok(())
	}
	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		let slot = (group, key.as_bytes().to_vec());
		if let Some(rn) = self.rows.get(&slot) {
			return Ok((RowNumber(*rn), false));
		}
		self.next_row += 1;
		self.rows.insert(slot, self.next_row);
		Ok((RowNumber(self.next_row), true))
	}
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		keys.iter().map(|k| self.get_or_create_row_number(group, k)).collect()
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.rows.remove(&(group, key.as_bytes().to_vec()));
		Ok(())
	}
	fn written_at(&self) -> DateTime {
		DateTime::EPOCH
	}
}

#[operator_state]
#[derive(Clone, Debug, Default)]
pub(crate) struct SumAccumulator {
	pub sum: i64,
	pub count: u64,
}

impl HeapSize for SumAccumulator {
	fn heap_size(&self) -> usize {
		0
	}
}

impl WindowAccumulator for SumAccumulator {
	type Contribution = i64;
	type Output = i64;

	fn add(&mut self, contribution: &i64) {
		self.sum += *contribution;
		self.count += 1;
	}
	fn remove(&mut self, contribution: &i64) {
		self.sum -= *contribution;
		self.count = self.count.saturating_sub(1);
	}
	fn finalize(&self) -> Option<i64> {
		if self.count == 0 {
			None
		} else {
			Some(self.sum)
		}
	}
	fn is_empty(&self) -> bool {
		self.count == 0
	}
	fn merge(&mut self, other: &Self) {
		self.sum += other.sum;
		self.count += other.count;
	}
	fn unmerge(&mut self, other: &Self) {
		self.sum -= other.sum;
		self.count = self.count.saturating_sub(other.count);
	}
}
