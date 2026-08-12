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

#[cfg(test)]
mod tests;

pub trait Reaper<S: StateStore + ?Sized> {
	fn reap(&mut self, store: &mut S, key: &GroupStateKey) -> Result<()>;
}

pub struct StoreReaper;

impl<S: StateStore + ?Sized> Reaper<S> for StoreReaper {
	fn reap(&mut self, store: &mut S, key: &GroupStateKey) -> Result<()> {
		store.state_remove(key)
	}
}

pub fn queue_key(group: GroupId) -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::REAP_QUEUE, encode_u64(group.0))
}

pub fn enqueue(store: &mut (impl StateStore + ?Sized), group: GroupId) -> Result<()> {
	let now = store.written_at();
	store.state_set(&queue_key(group), EncodedOperatorRow::new(&[], now))
}

pub fn queued(store: &mut (impl StateStore + ?Sized), limit: usize) -> Result<Vec<GroupId>> {
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

pub fn drain<S, R>(store: &mut S, reaper: &mut R, budget: usize) -> Result<usize>
where
	S: StateStore + ?Sized,
	R: Reaper<S>,
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
			store.state_remove(&queue_key(group))?;
		}
	}
	Ok(spent)
}

pub fn reap_group<S, R>(store: &mut S, group: GroupId, reaper: &mut R, budget: usize) -> Result<usize>
where
	S: StateStore + ?Sized,
	R: Reaper<S>,
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
