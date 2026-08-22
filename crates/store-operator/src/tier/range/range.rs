// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	ops::Bound::{self, Excluded, Included, Unbounded},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::{
	BUCKET_OVERHEAD, Bucket, BucketId, OperatorRangeTier, RangeFill, Shard, account, entry_footprint,
};

pub struct BucketScope {
	pub bucket: BucketId,
	pub whole: bool,
}

impl OperatorRangeTier {
	pub fn range(
		&self,
		operator: OperatorId,
		range: &EncodedKeyRange,
		limit: usize,
	) -> Option<Vec<(EncodedKey, EncodedPodRow)>> {
		let scope = bucket_scope(operator, range)?;
		let id = scope.bucket;
		let slot = id.keyspace.0 as usize;
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let items = {
			let Shard {
				buckets,
				metrics,
				keyspace_metrics,
				..
			} = &mut *shard;
			match buckets.get_mut(&id) {
				Some(bucket) => {
					bucket.tick = next;
					metrics.hits += 1;
					keyspace_metrics[slot].hits += 1;
					collect(bucket, range, limit)
				}
				None => {
					metrics.misses += 1;
					keyspace_metrics[slot].misses += 1;
					return None;
				}
			}
		};
		shard.next_tick = next + 1;
		Some(items)
	}

	pub fn lookup(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		let id = BucketId::of(operator, key)?;
		if !id.keyspace.is_cached() {
			return None;
		}
		let slot = id.keyspace.0 as usize;
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let found = {
			let Shard {
				buckets,
				metrics,
				keyspace_metrics,
				..
			} = &mut *shard;
			let Some(bucket) = buckets.get_mut(&id) else {
				metrics.point_misses += 1;
				keyspace_metrics[slot].point_misses += 1;
				return None;
			};
			bucket.tick = next;
			metrics.point_hits += 1;
			keyspace_metrics[slot].point_hits += 1;
			bucket.entries.get(key).cloned()
		};
		shard.next_tick = next + 1;
		Some(found)
	}

	pub fn begin_fill(&self, operator: OperatorId, range: &EncodedKeyRange) -> Option<BucketId> {
		let scope = bucket_scope(operator, range)?;
		if !scope.whole || !scope.bucket.keyspace.is_cached() {
			return None;
		}
		let id = scope.bucket;
		let mut shard = self.shard_for(&id).lock();
		if shard.filling.contains_key(&id) {
			return None;
		}
		shard.filling.insert(
			id,
			RangeFill {
				dirty: false,
				entries: BTreeMap::new(),
				bytes: BUCKET_OVERHEAD,
			},
		);
		Some(id)
	}

	pub fn extend_fill(&self, bucket: BucketId, page: &[(EncodedKey, EncodedPodRow)]) -> bool {
		let slot = bucket.keyspace.0 as usize;
		let mut shard = self.shard_for(&bucket).lock();
		let limit = shard.budget.limit().as_bytes() as usize;
		let Some(fill) = shard.filling.get_mut(&bucket) else {
			return false;
		};
		if fill.dirty {
			shard.filling.remove(&bucket);
			shard.metrics.fills_dirty_aborted += 1;
			shard.keyspace_metrics[slot].fills_dirty_aborted += 1;
			return false;
		}
		for (key, row) in page {
			fill.bytes += entry_footprint(key, row);
			fill.entries.insert(key.clone(), row.clone());
		}
		if fill.bytes <= limit {
			return true;
		}
		shard.filling.remove(&bucket);
		shard.metrics.fills_declined += 1;
		shard.keyspace_metrics[slot].fills_declined += 1;
		false
	}

	pub fn finish_fill(&self, bucket: BucketId) -> bool {
		let slot = bucket.keyspace.0 as usize;
		let mut shard = self.shard_for(&bucket).lock();
		let fill = match shard.filling.remove(&bucket) {
			Some(fill) if !fill.dirty => fill,
			Some(_) | None => {
				shard.metrics.fills_dirty_aborted += 1;
				shard.keyspace_metrics[slot].fills_dirty_aborted += 1;
				return false;
			}
		};
		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, bucket);
		}
		install(&mut shard, bucket, fill)
	}

	pub fn abort_fill(&self, bucket: BucketId) {
		self.shard_for(&bucket).lock().filling.remove(&bucket);
	}

	pub fn overwrite(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		let Some(id) = BucketId::of(operator, &key) else {
			return;
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(fill) = shard.filling.get_mut(&id) {
			fill.dirty = true;
		}
		let next = shard.next_tick;
		{
			let Shard {
				buckets,
				budget,
				..
			} = &mut *shard;
			let Some(bucket) = buckets.get_mut(&id) else {
				return;
			};
			let old = bucket.entries.get(&key).map_or(0, |previous| entry_footprint(&key, previous));
			let new = entry_footprint(&key, &row);
			bucket.entries.insert(key, row);
			account(&mut bucket.bytes, budget, old, new);
			bucket.tick = next;
		}
		shard.next_tick = next + 1;
		shard.evict_to_capacity();
	}

	pub fn invalidate(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(id) = BucketId::of(operator, key) else {
			return;
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(fill) = shard.filling.get_mut(&id) {
			fill.dirty = true;
		}
		let Shard {
			buckets,
			budget,
			..
		} = &mut *shard;
		if !buckets.get(&id).is_some_and(|bucket| bucket.entries.contains_key(key)) {
			return;
		}
		let Some(bucket) = buckets.remove(&id) else {
			return;
		};
		budget.release(ByteSize::from_bytes(bucket.bytes as u64));
	}

	pub fn invalidate_operator(&self, operator: OperatorId) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			for (bucket, fill) in shard.filling.iter_mut() {
				if bucket.operator == operator {
					fill.dirty = true;
				}
			}
			let Shard {
				buckets,
				budget,
				..
			} = &mut *shard;
			let victims: Vec<BucketId> =
				buckets.keys().filter(|id| id.operator == operator).copied().collect();
			for victim in victims {
				if let Some(bucket) = buckets.remove(&victim) {
					budget.release(ByteSize::from_bytes(bucket.bytes as u64));
				}
			}
		}
	}

	pub fn clear(&self) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			let Shard {
				buckets,
				budget,
				..
			} = &mut *shard;
			for bucket in buckets.values() {
				budget.release(ByteSize::from_bytes(bucket.bytes as u64));
			}
			buckets.clear();
			shard.filling.clear();
			shard.next_tick = 0;
		}
	}
}

fn install(shard: &mut Shard, id: BucketId, fill: RangeFill) -> bool {
	let slot = id.keyspace.0 as usize;
	let bytes = fill.bytes;
	if !shard.budget.try_charge(ByteSize::from_bytes(bytes as u64)) {
		shard.metrics.fills_declined += 1;
		shard.keyspace_metrics[slot].fills_declined += 1;
		return false;
	}
	let next = shard.next_tick;
	{
		let Shard {
			buckets,
			budget,
			..
		} = &mut *shard;
		if let Some(previous) = buckets.remove(&id) {
			budget.release(ByteSize::from_bytes(previous.bytes as u64));
		}
		buckets.insert(
			id,
			Bucket {
				entries: fill.entries,
				bytes,
				tick: next,
			},
		);
	}
	shard.next_tick = next + 1;
	shard.metrics.fills += 1;
	shard.keyspace_metrics[slot].fills += 1;
	shard.evict_to_capacity();
	true
}

fn collect(bucket: &Bucket, range: &EncodedKeyRange, limit: usize) -> Vec<(EncodedKey, EncodedPodRow)> {
	if !scannable(range.start.as_ref(), range.end.as_ref()) {
		return Vec::new();
	}
	bucket.entries
		.range::<EncodedKey, _>((range.start.as_ref(), range.end.as_ref()))
		.map(|(key, row)| (key.clone(), row.clone()))
		.take(limit)
		.collect()
}

fn scannable(start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> bool {
	let (Included(low) | Excluded(low), Included(high) | Excluded(high)) = (start, end) else {
		return true;
	};
	if low < high {
		return true;
	}
	low == high && !matches!((start, end), (Excluded(_), Excluded(_)))
}

pub fn bucket_scope(operator: OperatorId, range: &EncodedKeyRange) -> Option<BucketScope> {
	let start = match range.start.as_ref() {
		Included(key) | Excluded(key) => key,
		Unbounded => return None,
	};
	let bucket = BucketId::of(operator, start)?;
	let prefix = &start.as_slice()[..BucketId::PREFIX_LEN];
	let successor = prefix_successor(prefix)?;
	let ends_at_bucket_end = match range.end.as_ref() {
		Included(key) => {
			if !key.as_slice().starts_with(prefix) {
				return None;
			}
			false
		}
		Excluded(key) => {
			let at_bucket_end = key.as_slice() == successor.as_slice();
			if !key.as_slice().starts_with(prefix) && !at_bucket_end {
				return None;
			}
			at_bucket_end
		}
		Unbounded => return None,
	};
	let starts_at_bucket_start =
		matches!(range.start.as_ref(), Included(key) if key.as_slice().len() == BucketId::PREFIX_LEN);
	Some(BucketScope {
		bucket,
		whole: starts_at_bucket_start && ends_at_bucket_end,
	})
}

pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
	let mut out = prefix[..=last].to_vec();
	out[last] += 1;
	Some(out)
}
