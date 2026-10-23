// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_value::Result;

use crate::{
	actors::pending::PendingWrite,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceMask, keyspace_inner_range},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore, sweep_order},
	},
};

pub fn read_batch<S: StateStore + ?Sized>(store: &mut S, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
	let batch = StateBatch::read(keys, |keys| store.state_get(keys))?;
	for slot in 0..batch.len() {
		store.state_classify(batch.key(slot), batch.value(slot).map(EncodedPodRow::byte_size));
	}
	Ok(batch)
}

pub fn write_batch<S: StateStore + ?Sized>(store: &mut S, batch: StateBatch) -> Result<()> {
	let mut sets = Vec::new();
	let mut removes = Vec::new();
	for (key, write) in batch.into_writes() {
		match write {
			PendingWrite::Set(bytes) => sets.push((key, EncodedPodRow::from(bytes))),
			PendingWrite::Remove {
				..
			} => removes.push(key),
		}
	}
	if !sets.is_empty() {
		store.state_set(sets)?;
	}
	if !removes.is_empty() {
		store.state_remove(&removes)?;
	}
	Ok(())
}

pub fn group_sweep<S: StateStore + ?Sized>(
	store: &mut S,
	groups: &[GroupId],
	limit: usize,
	keyspaces: KeyspaceMask,
) -> Result<GroupSweep> {
	let probe = limit.saturating_add(1);
	let mut rows = Vec::new();
	for group in sweep_order(groups) {
		for keyspace in keyspaces.held().into_iter().rev() {
			if rows.len() >= probe {
				break;
			}
			let remaining = probe - rows.len();
			rows.extend(store.state_page_inner(keyspace_inner_range(group, keyspace), Some(remaining))?);
		}
	}
	let complete = rows.len() <= limit;
	rows.truncate(limit);
	Ok(GroupSweep {
		rows,
		complete,
	})
}
