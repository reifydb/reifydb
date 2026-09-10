// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::Bound,
	sync::Arc,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, group_inner_range},
	metrics::collect::MetricsCollector,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	error::Result,
	persistent::{Apply, Checkpoint, Enumerate, Fetch, Measure, Page, Persistent},
	store::occupancy::occupies,
	types::{Applied, DropMarker, FlushBatch, OperatorBatch, OperatorStateCensus, StagedWrite},
};

type Rows = BTreeMap<OperatorId, BTreeMap<GroupStateKey, EncodedPodRow>>;

#[derive(Default)]
struct Inner {
	rows: Mutex<Rows>,
	checkpoints: Mutex<BTreeMap<FlowId, CommitVersion>>,
}

#[derive(Clone, Default)]
pub struct MemoryPersistent(Arc<Inner>);

impl MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::new", level = "trace", skip_all)]
	pub fn new() -> Self {
		Self::default()
	}
}

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

#[instrument(name = "store::operator::persistent::memory::row_bytes", level = "trace", skip_all)]
fn row_bytes(key: &GroupStateKey, row: &EncodedPodRow) -> u64 {
	key.as_slice().len() as u64 + row.len() as u64
}

impl Persistent for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::metrics_collectors", level = "trace", skip_all)]
	fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		Vec::new()
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

impl Measure for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::state_sizes", level = "trace", skip_all)]
	fn state_sizes(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, ByteSize>> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(HashMap::new());
		};
		Ok(keys.iter()
			.filter_map(|key| {
				rows.get(key).map(|row| (key.clone(), ByteSize::from_bytes(row.len() as u64)))
			})
			.collect())
	}

	#[instrument(name = "store::operator::persistent::memory::bytes", level = "trace", skip_all)]
	fn bytes(&self, operator: OperatorId) -> Result<ByteSize> {
		let rows = self.0.rows.lock();
		let total = rows
			.get(&operator)
			.map(|rows| rows.iter().map(|(key, row)| row_bytes(key, row)).sum())
			.unwrap_or(0u64);
		Ok(ByteSize::from_bytes(total))
	}
}

impl Enumerate for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::census", level = "trace", skip_all)]
	fn census(&self) -> Result<Vec<OperatorStateCensus>> {
		let rows = self.0.rows.lock();
		let mut counted: BTreeMap<(OperatorId, u8), OperatorStateCensus> = BTreeMap::new();
		for (operator, rows) in rows.iter() {
			for (key, row) in rows.iter() {
				let Some(keyspace) = key.keyspace() else {
					continue;
				};
				let entry = counted.entry((*operator, keyspace.0)).or_insert(OperatorStateCensus {
					operator: *operator,
					keyspace,
					keys: 0,
					key_bytes: ByteSize::from_bytes(0),
					value_bytes: ByteSize::from_bytes(0),
				});
				entry.keys += 1;
				entry.key_bytes =
					ByteSize::from_bytes(entry.key_bytes.as_bytes() + key.as_slice().len() as u64);
				entry.value_bytes =
					ByteSize::from_bytes(entry.value_bytes.as_bytes() + row.len() as u64);
			}
		}
		Ok(counted.into_values().collect())
	}

	#[instrument(name = "store::operator::persistent::memory::operators", level = "trace", skip_all)]
	fn operators(&self) -> Result<Vec<OperatorId>> {
		Ok(self.0.rows.lock().keys().copied().collect())
	}

	#[instrument(name = "store::operator::persistent::memory::keyspaces", level = "trace", skip_all)]
	fn keyspaces(&self, operator: OperatorId) -> Result<Vec<KeyspaceId>> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(Vec::new());
		};
		let mut seen: Vec<KeyspaceId> = rows.keys().filter_map(|key| key.keyspace()).collect();
		seen.sort_unstable();
		seen.dedup();
		Ok(seen)
	}
}

impl Checkpoint for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::checkpoint_get", level = "trace", skip_all)]
	fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		Ok(self.0.checkpoints.lock().get(&flow).copied())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_set", level = "trace", skip_all)]
	fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()> {
		self.0.checkpoints.lock().insert(flow, version);
		Ok(())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_remove", level = "trace", skip_all)]
	fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		self.0.checkpoints.lock().remove(&flow);
		Ok(())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_floor", level = "trace", skip_all)]
	fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		Ok(self.0.checkpoints.lock().values().copied().min())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_list", level = "trace", skip_all)]
	fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		Ok(self.0.checkpoints.lock().keys().copied().collect())
	}
}

impl Apply for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::apply", level = "trace", skip_all)]
	fn apply(&self, batch: &FlushBatch) -> Result<Applied> {
		let mut rows = self.0.rows.lock();
		let mut checkpoints = self.0.checkpoints.lock();
		let mut applied = Applied::default();
		let mut bytes = 0u64;
		for drop in &batch.drops {
			let DropMarker::OperatorState(operator) = drop;
			rows.remove(operator);
		}
		for (operator, key, write) in &batch.writes {
			match write {
				StagedWrite::Set(row) => {
					bytes += row_bytes(key, row);
					rows.entry(*operator).or_default().insert(key.clone(), row.clone());
				}
				StagedWrite::Remove => {
					if let Some(held) = rows.get_mut(operator) {
						held.remove(key);
						if held.is_empty() {
							rows.remove(operator);
						}
					}
				}
			}
			applied.rows += 1;
		}
		for (flow, version) in &batch.checkpoints {
			match version {
				Some(version) => checkpoints.insert(*flow, *version),
				None => checkpoints.remove(flow),
			};
		}
		applied.bytes = ByteSize::from_bytes(bytes);
		Ok(applied)
	}

	#[instrument(name = "store::operator::persistent::memory::drop_operator", level = "trace", skip_all)]
	fn drop_operator(&self, operator: OperatorId) -> Result<()> {
		self.0.rows.lock().remove(&operator);
		Ok(())
	}
}
