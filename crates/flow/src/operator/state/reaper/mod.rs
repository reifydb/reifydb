// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	key::{
		operator::{
			keyspace::expiry::{ReapQueue, ReapQueueKey},
			state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, OperatorStateKey},
		},
		typed::direction::Desc,
	},
	state::{
		timer::StateStore,
		typed::{TypedStateStore, typed_key},
	},
};
use reifydb_value::{Result, reifydb_assertions};

use crate::operator::state::reclaim::ReclaimOutcome;

#[cfg(test)]
mod tests;

pub const UNIVERSAL: KeyspaceMask = KeyspaceMask::of(&[
	KeyspaceId::TIMER_WHEEL,
	KeyspaceId::TIMER_INDEX,
	KeyspaceId::GROUP_ROW_MAPPING,
	KeyspaceId::JOIN_ROW_MAPPING,
	KeyspaceId::GUEST_ROW_MAPPING,
	KeyspaceId::REAP_QUEUE,
	KeyspaceId::NODE_COUNTER,
	KeyspaceId::SOURCE_WATERMARK,
	KeyspaceId::GATE_VISIBILITY,
	KeyspaceId::SEAL_LEDGER,
]);

pub const WINDOW: KeyspaceMask = UNIVERSAL.union(KeyspaceMask::of(&[
	KeyspaceId::ACCUMULATOR,
	KeyspaceId::BUFFER,
	KeyspaceId::RUNNING,
	KeyspaceId::COUNT,
	KeyspaceId::SESSION,
	KeyspaceId::ROLLING_META,
	KeyspaceId::ENGINE_META,
	KeyspaceId::EMIT,
	KeyspaceId::ROW_INDEX,
	KeyspaceId::WINDOW_META,
	KeyspaceId::ROLLING_EXPIRY,
	KeyspaceId::TUMBLING_EXPIRY,
]));

pub const JOIN: KeyspaceMask = UNIVERSAL.union(KeyspaceMask::of(&[
	KeyspaceId::JOIN_LEFT,
	KeyspaceId::JOIN_RIGHT,
	KeyspaceId::JOIN_PUBLISHED,
	KeyspaceId::JOIN_PIN,
	KeyspaceId::JOIN_SCHEMA,
	KeyspaceId::JOIN_ROW_EXPIRY,
	KeyspaceId::JOIN_EXPIRY_DUE,
]));

pub const GUEST: KeyspaceMask = WINDOW.union(KeyspaceMask::of(&[
	KeyspaceId::GUEST_ACCUMULATOR,
	KeyspaceId::GUEST_BUFFER,
	KeyspaceId::GUEST_RUNNING,
	KeyspaceId::CUSTOM_NOT_CACHED,
]));

pub trait Reaper {
	fn reap(&mut self, store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()>;
}

pub trait IdentityReclaim: StateStore {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;

	fn reclaim_identity_keys(&mut self, group: GroupId, keys: &[GroupStateKey]) -> Result<ReclaimOutcome>;
}

pub struct StoreReaper;

impl Reaper for StoreReaper {
	fn reap(&mut self, store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()> {
		store.state_remove(key)
	}
}

pub fn queue_key(group: GroupId) -> GroupStateKey {
	typed_key::<ReapQueue>(
		GroupId::ROOT,
		&ReapQueueKey {
			group: Desc(group),
		},
	)
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
	let page = store.state_scan_in::<ReapQueue>(GroupId::ROOT, Bound::Unbounded, Some(limit.saturating_add(1)))?;
	let more = page.len() > limit;
	let groups = page.into_iter().take(limit).map(|(suffix, _)| suffix.group.0).collect();
	Ok(Queued {
		groups,
		more,
	})
}

pub struct GroupDrain {
	pub freed: usize,
	pub still_queued: bool,
}

struct GroupScan {
	identity: Vec<GroupStateKey>,
	data: Vec<(GroupStateKey, EncodedPodRow)>,
}

fn scan_group(
	store: &mut dyn StateStore,
	group: GroupId,
	mask: KeyspaceMask,
	budget: usize,
) -> Result<Option<GroupScan>> {
	let mut identity = Vec::new();
	let mut data = Vec::new();
	let swept = store.group_sweep_in(group, mask, false, Some(budget.saturating_add(1)))?;
	if swept.len() > budget {
		return Ok(None);
	}
	for (key, row) in swept {
		match OperatorStateKey::decode_inner(key.as_encoded().as_bytes()) {
			Some((_, keyspace, _)) if keyspace.is_data() => data.push((key, row)),
			Some(_) => identity.push(key),
			None => {}
		}
	}
	Ok(Some(GroupScan {
		identity,
		data,
	}))
}

pub fn drain_group<R>(
	store: &mut dyn IdentityReclaim,
	group: GroupId,
	mask: KeyspaceMask,
	reaper: &mut R,
	budget: usize,
) -> Result<GroupDrain>
where
	R: Reaper,
{
	let Some(scan) = scan_group(store, group, mask, budget)? else {
		return drain_group_scanning(store, group, mask, reaper, budget);
	};
	store.remove_root_siblings(&scan.data)?;
	for (key, _) in &scan.data {
		reaper.reap(store, key)?;
	}
	reifydb_assertions! {
		let leftover = store.group_sweep_in(group, mask, true, None)?.len();
		assert!(
			leftover == 0,
			"group {} still holds {leftover} data rows in its own partition; forgetting its dictionary \
			 entry now would orphan them behind a group id nothing can resolve again",
			group
		);
	}
	let freed = scan.data.len();
	let outcome = store.reclaim_identity_keys(group, &scan.identity)?;
	store.state_remove(&queue_key(group))?;
	Ok(GroupDrain {
		freed: freed + outcome.removed.as_u64() as usize,
		still_queued: false,
	})
}

fn drain_group_scanning<R>(
	store: &mut dyn IdentityReclaim,
	group: GroupId,
	mask: KeyspaceMask,
	reaper: &mut R,
	budget: usize,
) -> Result<GroupDrain>
where
	R: Reaper,
{
	let freed = reap_group(store, group, mask, reaper, budget)?;
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

pub fn drain<R>(
	store: &mut dyn IdentityReclaim,
	mask: KeyspaceMask,
	reaper: &mut R,
	budget: usize,
) -> Result<DrainOutcome>
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
		let outcome = drain_group(store, group, mask, reaper, allowance)?;
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

pub fn reap_group<R>(
	store: &mut dyn StateStore,
	group: GroupId,
	mask: KeyspaceMask,
	reaper: &mut R,
	budget: usize,
) -> Result<usize>
where
	R: Reaper,
{
	let doomed = store.group_sweep_in(group, mask, true, Some(budget))?;
	store.remove_root_siblings(&doomed)?;
	for (key, _) in &doomed {
		reaper.reap(store, key)?;
	}
	Ok(doomed.len())
}
