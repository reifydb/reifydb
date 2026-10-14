// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, keyspace_inner_range_split},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore, TimerKind, TimerStore, sweep_order},
	},
};
use reifydb_flow_async::operator::state::{reaper::IdentityReclaim, reclaim::ReclaimOutcome};
use reifydb_value::{
	Result,
	util::hash::xxh3_128,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::flow::operator::context::{GuestBound, GuestContext, GuestState};

pub struct GuestAsHost<'a, C: GuestContext>(pub &'a mut C);

fn confine(range: &EncodedKeyRange) -> (GroupId, KeyspaceId, Bound<Vec<u8>>, Bound<Vec<u8>>) {
	keyspace_inner_range_split(range).expect(
		"a guest state scan must stay inside one group and keyspace; the window engine builds only \
		 keyspace ranges, so a range that does not split is one a caller invented and the guest \
		 boundary cannot admit",
	)
}

fn mapping_key(key: &EncodedKey) -> EncodedKey {
	EncodedKey::builder().u128(xxh3_128(key.as_slice()).0).build()
}

impl<C: GuestContext> TimerStore for GuestAsHost<'_, C> {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.0.arm_timer(due, kind, key)?;
		Ok(())
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.0.disarm_timer(due, kind, key)?;
		Ok(())
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.0.flow_watermark()?)
	}
}

impl<C: GuestContext> IdentityReclaim for GuestAsHost<'_, C> {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		Ok(self.0.reclaim_group_identity(group, limit)?)
	}
}

impl<C: GuestContext> StateStore for GuestAsHost<'_, C> {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(self.0.window_state().get_bytes(key)?)
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		let mut found: HashMap<GroupStateKey, EncodedPodRow> = HashMap::new();
		self.0.window_state().get_many_bytes_visit(keys, &mut |key, row| {
			found.insert(key, row);
			Ok(())
		})?;
		Ok(keys.iter().map(|key| found.get(key).cloned()).collect())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		self.0.window_state().set_bytes(key, payload)?;
		Ok(())
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.0.window_state().remove_bytes(key)?;
		Ok(())
	}

	fn state_remove_many(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		self.0.window_state().remove_many_bytes(keys)?;
		Ok(())
	}

	fn state_set_many(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		let (keys, payloads): (Vec<GroupStateKey>, Vec<EncodedPodRow>) = rows.into_iter().unzip();
		let slots = keys.len();
		let mut batch = StateBatch::from_parts(keys, vec![None; slots], vec![None; slots])?;
		for (slot, payload) in payloads.into_iter().enumerate() {
			batch.set(slot, payload);
		}
		self.0.window_state().write_batch_bytes(batch)?;
		Ok(())
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		Ok(self.0.window_state().batch_bytes(keys)?)
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
		self.0.window_state().write_batch_bytes(batch)?;
		Ok(())
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let (group, keyspace, start, end) = confine(&range);
		let mut out = Vec::new();
		self.0.window_state().range_bytes_visit(
			group,
			keyspace,
			GuestBound::of(&start),
			GuestBound::of(&end),
			limit,
			&mut |k, v| {
				out.push((k, v));
				Ok(())
			},
		)?;
		Ok(out)
	}

	fn group_sweep(
		&mut self,
		group: GroupId,
		keyspaces: KeyspaceMask,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let (rows, _) =
			self.0.window_state().sweep_many_bytes(&[group], limit.unwrap_or(usize::MAX), keyspaces)?;
		Ok(rows)
	}

	fn group_sweep_many(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		let (rows, complete) =
			self.0.window_state().sweep_many_bytes(&sweep_order(groups), limit, keyspaces)?;
		Ok(GroupSweep {
			rows,
			complete,
		})
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let (group, keyspace, start, end) = confine(&range);
		Ok(self.0.window_state().last_bytes(group, keyspace, GuestBound::of(&start), GuestBound::of(&end))?)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		let keys: Vec<EncodedKey> = keys.iter().map(mapping_key).collect();
		Ok(self.0.get_or_create_row_numbers(group, &keys)?)
	}

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers_for_groups(groups)?)
	}

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		let keys: Vec<EncodedKey> = keys.iter().map(mapping_key).collect();
		Ok(self.0.get_row_numbers(group, &keys)?)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		Ok(self.0.remove_row_number(group, &mapping_key(key))?)
	}

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
		Ok(self.0.remove_row_number(group, &EncodedKey::new(Vec::new()))?)
	}

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		let keys: Vec<EncodedKey> = keys.iter().map(mapping_key).collect();
		Ok(self.0.remove_row_numbers(group, &keys)?)
	}

	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		Ok(self.0.remove_row_numbers_for_groups(groups)?)
	}

	fn written_at(&self) -> DateTime {
		self.0.written_at()
	}
}
