// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey},
	state::timer::{StateStore, TimerKind, TimerStore},
};
use reifydb_flow::operator::state::{reaper::IdentityReclaim, reclaim::ReclaimOutcome};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::flow::operator::context::{GuestContext, GuestState, KeyBound};

pub struct GuestAsHost<'a, C: GuestContext>(pub &'a mut C);

enum OwnedBound {
	Included(GroupStateKey),
	Excluded(GroupStateKey),
}

impl OwnedBound {
	fn of(bound: &Bound<EncodedKey>) -> Self {
		match bound {
			Bound::Included(key) => Self::Included(GroupStateKey::bound_unchecked(key.clone())),
			Bound::Excluded(key) => Self::Excluded(GroupStateKey::bound_unchecked(key.clone())),
			Bound::Unbounded => unreachable!(
				"a guest state scan must name both ends; the window engine builds only keyspace \
				 ranges, so an unbounded end means a caller invented a range the guest boundary \
				 cannot admit"
			),
		}
	}

	fn borrow(&self) -> KeyBound<'_> {
		match self {
			Self::Included(key) => KeyBound::Included(key),
			Self::Excluded(key) => KeyBound::Excluded(key),
		}
	}
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
		self.0.state().remove(key)?;
		Ok(())
	}

	fn state_page(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let (start, end) = (OwnedBound::of(&range.start), OwnedBound::of(&range.end));
		let mut out = Vec::new();
		self.0.state().range_bytes_visit(start.borrow(), end.borrow(), limit, &mut |k, v| {
			out.push((k, v));
			Ok(())
		})?;
		Ok(out)
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let (start, end) = (OwnedBound::of(&range.start), OwnedBound::of(&range.end));
		Ok(self.0.state().last_bytes(start.borrow(), end.borrow())?)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers(group, keys)?)
	}

	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers_for_pairs(pairs)?)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		Ok(self.0.remove_row_number(group, key)?)
	}

	fn written_at(&self) -> DateTime {
		self.0.written_at()
	}
}
