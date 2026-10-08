// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_value::Result;

use crate::{
	actors::pending::PendingWrite,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceMask, keyspace_inner_range},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore, sweep_order},
	},
};

pub fn remove_many<S: StateStore + ?Sized>(store: &mut S, keys: &[GroupStateKey]) -> Result<()> {
	for key in keys {
		store.state_remove(key)?;
	}
	Ok(())
}

pub fn read_batch<S: StateStore + ?Sized>(store: &mut S, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
	let batch = StateBatch::read(keys, |keys| store.state_get_many(keys))?;
	for slot in 0..batch.len() {
		store.state_classify(batch.key(slot), batch.value(slot).map(EncodedPodRow::byte_size));
	}
	Ok(batch)
}

pub fn write_batch<S: StateStore + ?Sized>(store: &mut S, batch: StateBatch) -> Result<()> {
	for (key, write) in batch.into_writes() {
		match write {
			PendingWrite::Set(bytes) => store.state_set(&key, EncodedPodRow::from(bytes))?,
			PendingWrite::Remove {
				..
			} => store.state_remove(&key)?,
		}
	}
	Ok(())
}

pub fn set_many<S: StateStore + ?Sized>(store: &mut S, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
	for (key, row) in rows {
		store.state_set(&key, row)?;
	}
	Ok(())
}

pub fn group_sweep<S: StateStore + ?Sized>(
	store: &mut S,
	group: GroupId,
	keyspaces: KeyspaceMask,
	limit: Option<usize>,
) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
	let mut rows = Vec::new();
	for keyspace in keyspaces.held().into_iter().rev() {
		let remaining = match limit {
			Some(limit) if rows.len() >= limit => break,
			Some(limit) => Some(limit - rows.len()),
			None => None,
		};
		rows.extend(store.state_page_inner(keyspace_inner_range(group, keyspace), remaining)?);
	}
	Ok(rows)
}

pub fn group_sweep_many<S: StateStore + ?Sized>(
	store: &mut S,
	groups: &[GroupId],
	limit: usize,
	keyspaces: KeyspaceMask,
) -> Result<GroupSweep> {
	let mut rows = Vec::new();
	for group in sweep_order(groups) {
		if rows.len() > limit {
			break;
		}
		let remaining = limit.saturating_add(1).saturating_sub(rows.len());
		rows.extend(store.group_sweep(group, keyspaces, Some(remaining))?);
	}
	let complete = rows.len() <= limit;
	rows.truncate(limit);
	Ok(GroupSweep {
		rows,
		complete,
	})
}

pub fn remove_row_numbers<S: StateStore + ?Sized>(store: &mut S, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
	for key in keys {
		store.remove_row_number(group, key)?;
	}
	Ok(())
}

pub fn remove_row_numbers_for_groups<S: StateStore + ?Sized>(store: &mut S, groups: &[GroupId]) -> Result<()> {
	for group in groups {
		store.remove_row_number_for_group(*group)?;
	}
	Ok(())
}
