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
	fn get<T: OperatorState>(&self, _keys: &[GroupStateKey]) -> Result<Vec<Option<T>>> {
		Ok(Vec::new())
	}

	fn set<T: OperatorState>(&mut self, _rows: &[(GroupStateKey, T)]) -> Result<()> {
		Ok(())
	}

	fn remove(&mut self, _keys: &[GroupStateKey]) -> Result<()> {
		Ok(())
	}

	fn contains(&self, _keys: &[GroupStateKey]) -> Result<Vec<bool>> {
		Ok(Vec::new())
	}

	fn get_bytes(
		&self,
		_keys: &[GroupStateKey],
		_visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		Ok(())
	}

	fn set_bytes(&mut self, _rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		Ok(())
	}

	fn remove_bytes(&mut self, _keys: &[GroupStateKey]) -> Result<()> {
		Ok(())
	}

	fn batch_bytes(&mut self, _keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		unimplemented!()
	}

	fn write_batch_bytes(&mut self, _batch: StateBatch) -> Result<()> {
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
