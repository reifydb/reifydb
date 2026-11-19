// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, slice::from_ref};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	key::{
		operator::{
			keyspace::due::{FORGOTTEN_AT_FLUSH, ForgetPassQueue, ForgetPassQueueKey},
			state::{GroupId, GroupStateKey, KeyspaceMask, OperatorStateKey},
		},
		typed::direction::Desc,
	},
	state::{
		timer::{StateStore, sweep_order},
		typed::{TypedStateStore, typed_key},
	},
};
use reifydb_value::{error::ValueError, reifydb_assertions};

use crate::operator::state::forget_pass_outcome::ForgetPassOutcome;

#[cfg(test)]
mod tests;

pub trait Forgetter {
	fn forget(
		&mut self,
		store: &mut dyn StateStore,
		rows: &[(GroupStateKey, EncodedPodRow)],
	) -> Result<(), ValueError>;
}

pub trait IdentityForgetPass: StateStore {
	fn forget_identities(&mut self, groups: &[GroupId], limit: usize)
	-> Result<Vec<ForgetPassOutcome>, ValueError>;
}

pub struct StoreForgetter;

impl Forgetter for StoreForgetter {
	fn forget(
		&mut self,
		store: &mut dyn StateStore,
		rows: &[(GroupStateKey, EncodedPodRow)],
	) -> Result<(), ValueError> {
		let keys: Vec<GroupStateKey> = rows.iter().map(|(key, _)| key.clone()).collect();
		store.state_remove(&keys)
	}
}

pub fn queue_key(group: GroupId) -> GroupStateKey {
	typed_key::<ForgetPassQueue>(
		GroupId::ROOT,
		&ForgetPassQueueKey {
			group: Desc(group),
		},
	)
}

pub fn enqueue(store: &mut dyn StateStore, groups: &[GroupId]) -> Result<(), ValueError> {
	reifydb_assertions! {
		for group in groups {
			assert!(
				!group.is_root(),
				"group id 0 is the root group; forgetting it would delete the forget pass queue, the due indexes and the seal ledger"
			);
		}
	}
	let rows: Vec<(GroupStateKey, EncodedPodRow)> = groups
		.iter()
		.filter(|group| !group.is_root())
		.map(|group| (queue_key(*group), EncodedPodRow::new(&[])))
		.collect();
	if rows.is_empty() {
		return Ok(());
	}
	store.state_set(rows)
}

pub fn enqueue_forgotten(store: &mut dyn StateStore, groups: &[GroupId]) -> Result<(), ValueError> {
	let rows: Vec<(GroupStateKey, EncodedPodRow)> = groups
		.iter()
		.filter(|group| !group.is_root())
		.map(|group| (queue_key(*group), EncodedPodRow::new(FORGOTTEN_AT_FLUSH)))
		.collect();
	if rows.is_empty() {
		return Ok(());
	}
	store.state_set(rows)
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

pub fn queued(store: &mut dyn StateStore, limit: usize) -> Result<Queued, ValueError> {
	let page =
		store.state_scan_in::<ForgetPassQueue>(GroupId::ROOT, Bound::Unbounded, Some(limit.saturating_add(1)))?;
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

#[derive(Default)]
struct GroupScan {
	data: Vec<(GroupStateKey, EncodedPodRow)>,
}

fn bucket(rows: Vec<(GroupStateKey, EncodedPodRow)>) -> (HashMap<GroupId, GroupScan>, Option<GroupId>) {
	let mut buckets: HashMap<GroupId, GroupScan> = HashMap::new();
	let mut last = None;
	for (key, row) in rows {
		let Some((group, _, _)) = OperatorStateKey::decode_inner(key.as_encoded().as_bytes()) else {
			continue;
		};
		last = Some(group);
		buckets.entry(group).or_default().data.push((key, row));
	}
	(buckets, last)
}

fn scan_group(store: &mut dyn StateStore, group: GroupId, budget: usize) -> Result<Option<GroupScan>, ValueError> {
	let swept = store.group_sweep(&[group], budget, KeyspaceMask::data())?;
	if !swept.complete {
		return Ok(None);
	}
	Ok(Some(GroupScan {
		data: swept.rows,
	}))
}

pub fn drain_group<R>(
	store: &mut dyn IdentityForgetPass,
	group: GroupId,
	forgetter: &mut R,
	budget: usize,
) -> Result<GroupDrain, ValueError>
where
	R: Forgetter,
{
	let Some(scan) = scan_group(store, group, budget)? else {
		return drain_group_scanning(store, group, forgetter, budget);
	};
	forget_scanned(store, group, scan, forgetter, budget)
}

fn forget_scanned<R>(
	store: &mut dyn IdentityForgetPass,
	group: GroupId,
	scan: GroupScan,
	forgetter: &mut R,
	budget: usize,
) -> Result<GroupDrain, ValueError>
where
	R: Forgetter,
{
	reifydb_assertions! {
		assert!(
			!group.is_root(),
			"group id 0 is the root group; forgetting it would delete the forget pass queue, the due indexes and the seal ledger"
		);
	}
	if group.is_root() {
		store.state_remove(from_ref(&queue_key(group)))?;
		return Ok(GroupDrain {
			freed: 0,
			still_queued: false,
		});
	}
	store.remove_root_siblings(&scan.data)?;
	forgetter.forget(store, &scan.data)?;
	reifydb_assertions! {
		let leftover = store.group_sweep(&[group], usize::MAX, KeyspaceMask::data())?.rows.len();
		assert!(
			leftover == 0,
			"group {} still holds {leftover} data rows in its own partition; forgetting its dictionary \
			 entry now would orphan them behind a group id nothing can resolve again",
			group
		);
	}
	let freed = scan.data.len();
	if freed >= budget {
		return Ok(GroupDrain {
			freed,
			still_queued: true,
		});
	}
	let outcome = store.forget_identities(from_ref(&group), budget - freed)?.remove(0);
	let freed = freed + outcome.removed.as_u64() as usize;
	if outcome.more {
		return Ok(GroupDrain {
			freed,
			still_queued: true,
		});
	}
	store.state_remove(from_ref(&queue_key(group)))?;
	Ok(GroupDrain {
		freed,
		still_queued: false,
	})
}

fn drain_group_scanning<R>(
	store: &mut dyn IdentityForgetPass,
	group: GroupId,
	forgetter: &mut R,
	budget: usize,
) -> Result<GroupDrain, ValueError>
where
	R: Forgetter,
{
	let freed = forget_group(store, group, forgetter, budget)?;
	if freed >= budget {
		return Ok(GroupDrain {
			freed,
			still_queued: true,
		});
	}
	let outcome = store.forget_identities(from_ref(&group), budget - freed)?.remove(0);
	let freed = freed + outcome.removed.as_u64() as usize;
	if outcome.more {
		return Ok(GroupDrain {
			freed,
			still_queued: true,
		});
	}
	store.state_remove(from_ref(&queue_key(group)))?;
	Ok(GroupDrain {
		freed,
		still_queued: false,
	})
}

pub fn drain<R>(
	store: &mut dyn IdentityForgetPass,
	forgetter: &mut R,
	budget: usize,
) -> Result<DrainOutcome, ValueError>
where
	R: Forgetter,
{
	let scan = queued(store, budget)?;
	let mut outcome = drain_groups(store, &scan.groups, forgetter, budget)?;
	outcome.more = scan.more;
	Ok(outcome)
}

pub fn drain_groups<R>(
	store: &mut dyn IdentityForgetPass,
	groups: &[GroupId],
	forgetter: &mut R,
	budget: usize,
) -> Result<DrainOutcome, ValueError>
where
	R: Forgetter,
{
	let ordered = sweep_order(groups);
	let sweep = store.group_sweep(&ordered, budget, KeyspaceMask::data())?;
	let (mut buckets, last) = bucket(sweep.rows);
	let cut = match sweep.complete {
		true => None,
		false => last,
	};

	let mut planned: Vec<(GroupId, usize)> = Vec::new();
	let mut doomed: Vec<(GroupStateKey, EncodedPodRow)> = Vec::new();
	let mut pending = ordered.into_iter();
	let mut cut_group = None;
	for group in pending.by_ref() {
		if cut == Some(group) {
			cut_group = Some(group);
			break;
		}
		let scan = buckets.remove(&group).unwrap_or_default();
		reifydb_assertions! {
			assert!(
				!group.is_root(),
				"group id 0 is the root group; forgetting it would delete the forget pass queue, the due indexes and the seal ledger"
			);
		}
		if group.is_root() {
			planned.push((group, 0));
			continue;
		}
		planned.push((group, scan.data.len()));
		doomed.extend(scan.data);
	}

	if !doomed.is_empty() {
		store.remove_root_siblings(&doomed)?;
		forgetter.forget(store, &doomed)?;
	}

	let mut spent = 0usize;
	let mut still_queued: Vec<GroupId> = Vec::new();
	let mut forgetting: Vec<GroupId> = Vec::new();
	let mut finished: Vec<GroupStateKey> = Vec::new();
	for (group, freed) in planned {
		if group.is_root() {
			finished.push(queue_key(group));
			continue;
		}
		reifydb_assertions! {
			let leftover = store.group_sweep(&[group], usize::MAX, KeyspaceMask::data())?.rows.len();
			assert!(
				leftover == 0,
				"group {} still holds {leftover} data rows in its own partition; forgetting its dictionary \
				 entry now would orphan them behind a group id nothing can resolve again",
				group
			);
		}
		spent += freed;
		forgetting.push(group);
	}
	let available = budget.saturating_sub(spent);
	if available == 0 {
		still_queued.extend(forgetting);
	} else if !forgetting.is_empty() {
		let outcomes = store.forget_identities(&forgetting, available)?;
		for (group, outcome) in forgetting.into_iter().zip(outcomes) {
			spent += outcome.removed.as_u64() as usize;
			match outcome.more {
				true => still_queued.push(group),
				false => finished.push(queue_key(group)),
			}
		}
	}
	if !finished.is_empty() {
		store.state_remove(&finished)?;
	}

	if let Some(group) = cut_group {
		match spent {
			0 => {
				let outcome = drain_group_scanning(store, group, forgetter, budget)?;
				spent += outcome.freed;
				if outcome.still_queued {
					still_queued.push(group);
				}
			}
			_ => still_queued.push(group),
		}
		still_queued.extend(pending);
	}
	Ok(DrainOutcome {
		freed: spent,
		still_queued,
		more: false,
	})
}

pub fn forget_group<R>(
	store: &mut dyn StateStore,
	group: GroupId,
	forgetter: &mut R,
	budget: usize,
) -> Result<usize, ValueError>
where
	R: Forgetter,
{
	reifydb_assertions! {
		assert!(
			!group.is_root(),
			"group id 0 is the root group; forgetting it would delete the forget pass queue, the due indexes and the seal ledger"
		);
	}
	if group.is_root() {
		return Ok(0);
	}
	let doomed = store.group_sweep(&[group], budget, KeyspaceMask::data())?.rows;
	store.remove_root_siblings(&doomed)?;
	forgetter.forget(store, &doomed)?;
	Ok(doomed.len())
}
