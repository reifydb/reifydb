// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{decode_u64, encode_u64},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	key::operator_state::{
		GroupId, GroupStateKey, Keyspace, OperatorStateKey, group_inner_range, keyspace_inner_range,
	},
	state::store::StateStore,
};
use reifydb_value::Result;

use crate::operator::state::reclaim::ReclaimOutcome;

#[cfg(test)]
mod tests;

pub trait Reaper {
	fn reap(&mut self, store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()>;
}

pub trait IdentityReclaim: StateStore {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;
}

pub struct StoreReaper;

impl Reaper for StoreReaper {
	fn reap(&mut self, store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()> {
		store.state_remove(key)
	}
}

pub fn queue_key(group: GroupId) -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::REAP_QUEUE, encode_u64(group.0))
}

pub fn enqueue(store: &mut dyn StateStore, group: GroupId) -> Result<()> {
	let now = store.written_at();
	store.state_set(&queue_key(group), EncodedOperatorRow::new(&[], now))
}

pub fn queued(store: &mut dyn StateStore, limit: usize) -> Result<Vec<GroupId>> {
	let mut groups: Vec<GroupId> = Vec::new();
	store.state_range_visit(keyspace_inner_range(GroupId::ROOT, Keyspace::REAP_QUEUE), None, &mut |key, _| {
		if groups.len() < limit
			&& let Some((_, _, suffix)) = OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
			&& let Ok(bytes) = <[u8; 8]>::try_from(suffix.as_slice())
		{
			groups.push(GroupId(decode_u64(bytes)));
		}
		Ok(())
	})?;
	Ok(groups)
}

pub fn drain<R>(store: &mut dyn IdentityReclaim, reaper: &mut R, budget: usize) -> Result<usize>
where
	R: Reaper,
{
	let mut spent = 0usize;
	for group in queued(store, budget)? {
		let allowance = budget - spent;
		if allowance == 0 {
			break;
		}
		let freed = reap_group(store, group, reaper, allowance)?;
		spent += freed;
		if freed < allowance {
			let outcome = store.reclaim_identity(group, allowance - freed)?;
			spent += outcome.removed.as_u64() as usize;
			if !outcome.more {
				store.state_remove(&queue_key(group))?;
			}
		}
	}
	Ok(spent)
}

pub fn reap_group<R>(store: &mut dyn StateStore, group: GroupId, reaper: &mut R, budget: usize) -> Result<usize>
where
	R: Reaper,
{
	let mut doomed: Vec<GroupStateKey> = Vec::new();
	store.state_range_visit(group_inner_range(group), None, &mut |key, _payload| {
		if doomed.len() < budget
			&& OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
				.is_some_and(|(_, keyspace, _)| keyspace.is_data())
		{
			doomed.push(key);
		}
		Ok(())
	})?;
	for key in &doomed {
		reaper.reap(store, key)?;
	}
	Ok(doomed.len())
}
