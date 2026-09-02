// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, keyspace_inner_range_split},
	state::timer::{StateStore, TimerKind, TimerStore},
};
use reifydb_flow::operator::state::{reaper::IdentityReclaim, reclaim::ReclaimOutcome};
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

	fn reclaim_identity_keys(&mut self, group: GroupId, keys: &[GroupStateKey]) -> Result<ReclaimOutcome> {
		Ok(self.0.reclaim_group_identity_keys(group, keys)?)
	}
}

impl<C: GuestContext> StateStore for GuestAsHost<'_, C> {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(self.0.state().get_bytes(key)?)
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		self.0.state().get_many_bytes_visit(keys, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		self.0.state().set_bytes(key, payload)?;
		Ok(())
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.0.state().remove_bytes(key)?;
		Ok(())
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let (group, keyspace, start, end) = confine(&range);
		let mut out = Vec::new();
		self.0.state().range_bytes_visit(
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

	fn group_sweep_in(
		&mut self,
		group: GroupId,
		mask: KeyspaceMask,
		data_only: bool,
		limit: Option<usize>,
	) -> Result<Vec<GroupStateKey>> {
		let mut swept = Vec::new();
		self.0.state().sweep_bytes_visit(group, mask, data_only, limit, &mut |key, _| {
			swept.push(key);
			Ok(())
		})?;
		Ok(swept)
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let (group, keyspace, start, end) = confine(&range);
		Ok(self.0.state().last_bytes(group, keyspace, GuestBound::of(&start), GuestBound::of(&end))?)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		let keys: Vec<EncodedKey> = keys.iter().map(mapping_key).collect();
		Ok(self.0.get_or_create_row_numbers(group, &keys)?)
	}

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		let pairs: Vec<(GroupId, EncodedKey)> =
			groups.iter().map(|group| (*group, EncodedKey::new(Vec::new()))).collect();
		Ok(self.0.get_or_create_row_numbers_for_pairs(&pairs)?)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		Ok(self.0.remove_row_number(group, &mapping_key(key))?)
	}

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
		Ok(self.0.remove_row_number(group, &EncodedKey::new(Vec::new()))?)
	}

	fn written_at(&self) -> DateTime {
		self.0.written_at()
	}
}
