// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey},
	state::store::{StateStore, TimerKind, TimerStore},
};
use reifydb_flow::operator::state::{reaper::IdentityReclaim, reclaim::ReclaimOutcome};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::flow::operator::context::{GuestContext, GuestState};

pub struct GuestAsHost<'a, C: GuestContext>(pub &'a mut C);

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
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		Ok(self.0.state().get_bytes(key)?)
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		self.0.state().get_many_bytes_visit(keys, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
		self.0.state().set_bytes(key, payload)?;
		Ok(())
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.0.state().remove(key)?;
		Ok(())
	}

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let bound = |b: &Bound<EncodedKey>| match b {
			Bound::Included(k) => Bound::Included(GroupStateKey::bound_unchecked(k.clone())),
			Bound::Excluded(k) => Bound::Excluded(GroupStateKey::bound_unchecked(k.clone())),
			Bound::Unbounded => Bound::Unbounded,
		};
		let (start, end) = (bound(&range.start), bound(&range.end));
		let (start, end) = (start.as_ref(), end.as_ref());
		let mut remaining = limit;
		self.0.state().range_bytes_visit(start, end, &mut |k, v| match remaining.as_mut() {
			Some(0) => Ok(()),
			Some(r) => {
				*r -= 1;
				visit(k, v).map_err(Into::into)
			}
			None => visit(k, v).map_err(Into::into),
		})?;
		Ok(())
	}

	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
		Ok(self.0.intern_group(group)?)
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		Ok(self.0.lookup_group(group)?)
	}

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		Ok(self.0.get_or_create_row_number(group, key)?)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers(group, keys)?)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		Ok(self.0.remove_row_number(group, key)?)
	}

	fn written_at(&self) -> DateTime {
		self.0.written_at()
	}
}
