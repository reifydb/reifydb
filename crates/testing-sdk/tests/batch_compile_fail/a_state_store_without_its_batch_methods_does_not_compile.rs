// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey},
	state::timer::StateStore,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

// A store that only answers key by key must not compile, or it silently pays one call per key on every batch.
struct SingleKeyStore;

impl StateStore for SingleKeyStore {
	fn state_get(&mut self, _key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(None)
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		Ok(vec![None; keys.len()])
	}

	fn state_set(&mut self, _key: &GroupStateKey, _payload: EncodedPodRow) -> Result<()> {
		Ok(())
	}

	fn state_remove(&mut self, _key: &GroupStateKey) -> Result<()> {
		Ok(())
	}

	fn state_page_inner(
		&mut self,
		_range: EncodedKeyRange,
		_limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		Ok(Vec::new())
	}

	fn get_or_create_row_numbers(
		&mut self,
		_group: GroupId,
		_keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		Ok(Vec::new())
	}

	fn get_or_create_row_numbers_for_groups(&mut self, _groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(Vec::new())
	}

	fn remove_row_number(&mut self, _group: GroupId, _key: &EncodedKey) -> Result<()> {
		Ok(())
	}

	fn remove_row_number_for_group(&mut self, _group: GroupId) -> Result<()> {
		Ok(())
	}

	fn written_at(&self) -> DateTime {
		DateTime::EPOCH
	}
}

fn main() {}
