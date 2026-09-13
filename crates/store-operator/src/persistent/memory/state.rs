// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey, group_inner_range},
};
use tracing::instrument;

use crate::{
	error::Result,
	persistent::{Fetch, Page, memory::MemoryPersistent},
	store::occupancy::occupies,
	types::OperatorBatch,
};

#[instrument(name = "store::operator::persistent::memory::lift", level = "trace", skip_all)]
fn lift(bound: &Bound<EncodedKey>) -> Bound<GroupStateKey> {
	match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => Bound::Included(GroupStateKey::bound_unchecked(key.clone())),
		Bound::Excluded(key) => Bound::Excluded(GroupStateKey::bound_unchecked(key.clone())),
	}
}

#[instrument(name = "store::operator::persistent::memory::bounds", level = "trace", skip_all)]
fn bounds(range: &EncodedKeyRange) -> (Bound<GroupStateKey>, Bound<GroupStateKey>) {
	(lift(&range.start), lift(&range.end))
}

#[instrument(name = "store::operator::persistent::memory::group_order", level = "trace", skip_all)]
fn group_order(group: GroupId) -> Option<EncodedKey> {
	match group_inner_range(group).start {
		Bound::Unbounded => None,
		Bound::Included(key) | Bound::Excluded(key) => Some(key),
	}
}

#[instrument(name = "store::operator::persistent::memory::well_formed", level = "trace", skip_all)]
fn well_formed(bounds: &(Bound<GroupStateKey>, Bound<GroupStateKey>)) -> bool {
	match (&bounds.0, &bounds.1) {
		(Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
		(Bound::Included(low), Bound::Included(high)) => low <= high,
		(Bound::Included(low), Bound::Excluded(high))
		| (Bound::Excluded(low), Bound::Included(high))
		| (Bound::Excluded(low), Bound::Excluded(high)) => low < high,
	}
}

#[instrument(name = "store::operator::persistent::memory::limit_of", level = "trace", skip_all)]
fn limit_of(batch: u64) -> usize {
	usize::try_from(batch).unwrap_or(usize::MAX)
}

#[instrument(name = "store::operator::persistent::memory::fetch_of", level = "trace", skip_all)]
fn fetch_of(batch: u64) -> usize {
	limit_of(batch).saturating_add(1)
}

#[instrument(name = "store::operator::persistent::memory::into_batch", level = "trace", skip_all)]
fn into_batch(mut items: Vec<(GroupStateKey, EncodedPodRow)>, batch: u64) -> OperatorBatch {
	let limit = limit_of(batch);
	match items.len() > limit {
		true => {
			let resume = items[limit].0.clone();
			items.truncate(limit);
			OperatorBatch {
				items,
				has_more: true,
				resume: Some(resume),
			}
		}
		false => OperatorBatch {
			items,
			has_more: false,
			resume: None,
		},
	}
}

impl Fetch for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::get", level = "trace", skip_all)]
	fn get(&self, operator: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(self.0.rows.lock().get(&operator).and_then(|rows| rows.get(key)).cloned())
	}

	#[instrument(name = "store::operator::persistent::memory::get_many", level = "trace", skip_all)]
	fn get_many(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, EncodedPodRow>> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(HashMap::new());
		};
		Ok(keys.iter().filter_map(|key| rows.get(key).map(|row| (key.clone(), row.clone()))).collect())
	}

	#[instrument(name = "store::operator::persistent::memory::contains", level = "trace", skip_all)]
	fn contains(&self, operator: OperatorId, key: &GroupStateKey) -> Result<bool> {
		Ok(self.0.rows.lock().get(&operator).is_some_and(|rows| rows.contains_key(key)))
	}
}

impl Page for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::range_batch", level = "trace", skip_all)]
	fn range_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		_occupied: u64,
	) -> Result<OperatorBatch> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(OperatorBatch::empty());
		};
		let span = bounds(&range);
		if !well_formed(&span) {
			return Ok(OperatorBatch::empty());
		}
		let items =
			rows.range(span).take(fetch_of(batch)).map(|(key, row)| (key.clone(), row.clone())).collect();
		Ok(into_batch(items, batch))
	}

	#[instrument(name = "store::operator::persistent::memory::last_batch", level = "trace", skip_all)]
	fn last_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		_occupied: u64,
	) -> Result<OperatorBatch> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(OperatorBatch::empty());
		};
		let span = bounds(&range);
		if !well_formed(&span) {
			return Ok(OperatorBatch::empty());
		}
		let items: Vec<(GroupStateKey, EncodedPodRow)> = rows
			.range(span)
			.rev()
			.take(fetch_of(batch))
			.map(|(key, row)| (key.clone(), row.clone()))
			.collect();
		Ok(into_batch(items, batch))
	}

	#[instrument(name = "store::operator::persistent::memory::group_page", level = "trace", skip_all)]
	fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch: u64, mask: u64) -> Result<OperatorBatch> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(OperatorBatch::empty());
		};
		let mut ordered = groups.to_vec();
		ordered.sort_by_cached_key(|group| group_order(*group));
		let mut items = Vec::new();
		for group in &ordered {
			let span = bounds(&group_inner_range(*group));
			if !well_formed(&span) {
				continue;
			}
			for (key, row) in rows.range(span) {
				let Some(keyspace) = key.keyspace() else {
					continue;
				};
				if !occupies(mask, keyspace) {
					continue;
				}
				items.push((key.clone(), row.clone()));
				if items.len() > limit_of(batch) {
					return Ok(into_batch(items, batch));
				}
			}
		}
		Ok(into_batch(items, batch))
	}
}
