// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{decode_u64, encode_u64},
	row::pod::EncodedPodRow,
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
	store.state_set(&queue_key(group), EncodedPodRow::new(&[]))
}

pub struct Queued {
	pub groups: Vec<GroupId>,
	pub more: bool,
}

pub struct DrainOutcome {
	pub freed: usize,
	pub still_queued: Vec<GroupId>,
	pub more: bool,
}

impl DrainOutcome {
	pub fn queue_is_empty(&self) -> bool {
		self.still_queued.is_empty() && !self.more
	}
}

pub fn queued(store: &mut dyn StateStore, limit: usize) -> Result<Queued> {
	let mut groups: Vec<GroupId> = Vec::new();
	let mut more = false;
	store.state_range_visit(keyspace_inner_range(GroupId::ROOT, Keyspace::REAP_QUEUE), None, &mut |key, _| {
		if let Some((_, _, suffix)) = OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
			&& let Ok(bytes) = <[u8; 8]>::try_from(suffix.as_slice())
		{
			if groups.len() < limit {
				groups.push(GroupId(decode_u64(bytes)));
			} else {
				more = true;
			}
		}
		Ok(())
	})?;
	Ok(Queued {
		groups,
		more,
	})
}

pub struct GroupDrain {
	pub freed: usize,
	pub still_queued: bool,
}

pub fn drain_group<R>(
	store: &mut dyn IdentityReclaim,
	group: GroupId,
	reaper: &mut R,
	budget: usize,
) -> Result<GroupDrain>
where
	R: Reaper,
{
	let freed = reap_group(store, group, reaper, budget)?;
	if freed >= budget {
		return Ok(GroupDrain {
			freed,
			still_queued: true,
		});
	}
	let outcome = store.reclaim_identity(group, budget - freed)?;
	let freed = freed + outcome.removed.as_u64() as usize;
	if outcome.more {
		return Ok(GroupDrain {
			freed,
			still_queued: true,
		});
	}
	store.state_remove(&queue_key(group))?;
	Ok(GroupDrain {
		freed,
		still_queued: false,
	})
}

pub fn drain<R>(store: &mut dyn IdentityReclaim, reaper: &mut R, budget: usize) -> Result<DrainOutcome>
where
	R: Reaper,
{
	let scan = queued(store, budget)?;
	let mut spent = 0usize;
	let mut still_queued: Vec<GroupId> = Vec::new();
	let mut pending = scan.groups.into_iter();
	while let Some(group) = pending.next() {
		let allowance = budget - spent;
		if allowance == 0 {
			still_queued.push(group);
			still_queued.extend(pending);
			break;
		}
		let outcome = drain_group(store, group, reaper, allowance)?;
		spent += outcome.freed;
		if outcome.still_queued {
			still_queued.push(group);
		}
	}
	Ok(DrainOutcome {
		freed: spent,
		still_queued,
		more: scan.more,
	})
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
