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
	state::store::{StateStore, TimerKind, TimerStore},
};
use reifydb_value::{
	Result,
	count::Count,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	operator::state::{reaper::IdentityReclaim, reclaim::ReclaimOutcome},
	timer::Timer,
	window::accumulator::WindowAccumulator,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordedTimer {
	Armed(Timer),
	Disarmed(Timer),
}

impl RecordedTimer {
	pub(crate) fn armed(due: DateTime, kind: TimerKind, key: EncodedKey) -> Self {
		Self::Armed(Timer {
			due,
			kind,
			key,
		})
	}

	pub(crate) fn disarmed(due: DateTime, kind: TimerKind, key: EncodedKey) -> Self {
		Self::Disarmed(Timer {
			due,
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

impl IdentityReclaim for MockStore {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		let rows: Vec<Vec<u8>> =
			self.rows.keys().filter(|(owner, _)| *owner == group).map(|(_, key)| key.clone()).collect();
		let keys: Vec<Vec<u8>> = self
			.data
			.keys()
			.filter(|key| {
				OperatorStateKey::decode_inner(key)
					.is_some_and(|(owner, keyspace, _)| owner == group && keyspace.is_identity())
			})
			.cloned()
			.collect();
		let total = rows.len() + keys.len();
		let mut removed = 0usize;
		for key in rows.into_iter().take(limit) {
			self.rows.remove(&(group, key));
			removed += 1;
		}
		for key in keys.into_iter().take(limit.saturating_sub(removed)) {
			self.data.remove(&key);
			removed += 1;
		}
		let more = removed < total;
		if !more {
			self.groups.retain(|_, owner| *owner != group);
		}
		Ok(ReclaimOutcome {
			removed: Count::new(removed as u64),
			more,
		})
	}
}

impl TimerStore for MockStore {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record_timer(RecordedTimer::armed(due, kind, key.clone()))
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record_timer(RecordedTimer::disarmed(due, kind, key.clone()))
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
			let k = GroupStateKey::from_framed(EncodedKey::new(k))
				.expect("fake store holds an unframed state key");
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
