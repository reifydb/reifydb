// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{operator::state::OperatorState, pod::EncodedPodRow};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
	state::batch::StateBatch,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::context::{GuestBound, GuestState},
};

// A guest binding that sweeps group by group must not compile, or every reap crosses once per group unnoticed.
struct PerGroupState;

impl GuestState for PerGroupState {
	fn get<T: OperatorState>(&self, _key: &GroupStateKey) -> Result<Option<T>> {
		Ok(None)
	}

	fn set<T: OperatorState>(&mut self, _key: &GroupStateKey, _value: &T) -> Result<()> {
		Ok(())
	}

	fn remove(&mut self, _key: &GroupStateKey) -> Result<()> {
		Ok(())
	}

	fn contains(&self, _key: &GroupStateKey) -> Result<bool> {
		Ok(false)
	}

	fn get_bytes(&self, _key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(None)
	}

	fn set_bytes(&mut self, _key: &GroupStateKey, _payload: EncodedPodRow) -> Result<()> {
		Ok(())
	}

	fn remove_bytes(&mut self, _key: &GroupStateKey) -> Result<()> {
		Ok(())
	}

	fn remove_many_bytes(&mut self, _keys: &[GroupStateKey]) -> Result<()> {
		Ok(())
	}

	fn batch_bytes(&mut self, _keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		unimplemented!()
	}

	fn write_batch_bytes(&mut self, _batch: StateBatch) -> Result<()> {
		Ok(())
	}

	fn get_many_bytes_visit(
		&self,
		_keys: &[GroupStateKey],
		_visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		Ok(())
	}

	fn range_bytes_visit(
		&self,
		_group: GroupId,
		_keyspace: KeyspaceId,
		_start: GuestBound<'_>,
		_end: GuestBound<'_>,
		_limit: Option<usize>,
		_visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		Ok(())
	}
}

fn main() {}
