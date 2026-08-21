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

use crate::tier::read::{
	BUCKET_OVERHEAD, Bucket, BucketId, OperatorReadBufferTier, RangeFill, Shard, class_budget, entry_footprint,
};

pub struct BucketScope {
	pub bucket: BucketId,
	pub whole: bool,
}

impl OperatorReadBufferTier {
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
			match buckets.get_mut(&id).filter(|bucket| bucket.complete) {
				Some(bucket) => {
					bucket.tick = next;
					metrics.range_hits += 1;
					keyspace_metrics[slot].range_hits += 1;
					collect(bucket, range, limit)
				}
				None => {
					metrics.range_misses += 1;
					keyspace_metrics[slot].range_misses += 1;
					return None;
				}
			}
		};
		shard.next_tick = next + 1;
		Some(items)
	}

	pub fn begin_range_fill(&self, operator: OperatorId, range: &EncodedKeyRange) -> Option<BucketId> {
		let scope = bucket_scope(operator, range)?;
		if !scope.whole || !scope.bucket.keyspace.is_cached() {
			return None;
		}
		let id = scope.bucket;
		let mut shard = self.shard_for(&id).lock();
		if shard.range_filling.contains_key(&id) {
			return None;
		}
		shard.range_filling.insert(
			id,
			RangeFill {
				dirty: false,
				entries: BTreeMap::new(),
				bytes: BUCKET_OVERHEAD,
			},
		);
		Some(id)
	}

	pub fn extend_range_fill(&self, bucket: BucketId, page: &[(EncodedKey, EncodedPodRow)]) -> bool {
		let slot = bucket.keyspace.0 as usize;
		let mut shard = self.shard_for(&bucket).lock();
		let limit = shard.range_budget.limit().as_bytes() as usize;
		let Some(fill) = shard.range_filling.get_mut(&bucket) else {
			return false;
		};
		if fill.dirty {
			shard.range_filling.remove(&bucket);
			shard.metrics.range_fills_dirty_aborted += 1;
			shard.keyspace_metrics[slot].range_fills_dirty_aborted += 1;
			return false;
		}
		for (key, row) in page {
			let row = Some(row.clone());
			fill.bytes += entry_footprint(key, &row);
			fill.entries.insert(key.clone(), row);
		}
		if fill.bytes <= limit {
			return true;
		}
		shard.range_filling.remove(&bucket);
		shard.metrics.range_fills_declined += 1;
		shard.keyspace_metrics[slot].range_fills_declined += 1;
		false
	}

	pub fn finish_range_fill(&self, bucket: BucketId) -> bool {
		let slot = bucket.keyspace.0 as usize;
		let mut shard = self.shard_for(&bucket).lock();
		let fill = match shard.range_filling.remove(&bucket) {
			Some(fill) if !fill.dirty => fill,
			Some(_) | None => {
				shard.metrics.range_fills_dirty_aborted += 1;
				shard.keyspace_metrics[slot].range_fills_dirty_aborted += 1;
				return false;
			}
		};
		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, bucket);
		}
		install(&mut shard, bucket, fill)
	}

	pub fn abort_range_fill(&self, bucket: BucketId) {
		self.shard_for(&bucket).lock().range_filling.remove(&bucket);
	}
}

fn install(shard: &mut Shard, id: BucketId, mut fill: RangeFill) -> bool {
	let slot = id.keyspace.0 as usize;
	let carried: Vec<(EncodedKey, usize)> = shard
		.buckets
		.get(&id)
		.map(|previous| {
			previous.entries
				.iter()
				.filter(|(key, row)| row.is_none() && !fill.entries.contains_key(*key))
				.map(|(key, row)| (key.clone(), entry_footprint(key, row)))
				.collect()
		})
		.unwrap_or_default();
	let bytes = fill.bytes + carried.iter().map(|(_, footprint)| footprint).sum::<usize>();
	if !shard.range_budget.try_charge(ByteSize::from_bytes(bytes as u64)) {
		shard.metrics.range_fills_declined += 1;
		shard.keyspace_metrics[slot].range_fills_declined += 1;
		return false;
	}
	for (key, _) in carried {
		fill.entries.insert(key, None);
	}
	let next = shard.next_tick;
	{
		let Shard {
			buckets,
			budget,
			range_budget,
			..
		} = &mut *shard;
		if let Some(previous) = buckets.remove(&id) {
			class_budget(budget, range_budget, previous.complete)
				.release(ByteSize::from_bytes(previous.bytes as u64));
		}
		buckets.insert(
			id,
			Bucket {
				entries: fill.entries,
				bytes,
				complete: true,
				tick: next,
			},
		);
	}
	shard.next_tick = next + 1;
	shard.metrics.range_fills += 1;
	shard.keyspace_metrics[slot].range_fills += 1;
	shard.evict_to_capacity();
	true
}

fn collect(bucket: &Bucket, range: &EncodedKeyRange, limit: usize) -> Vec<(EncodedKey, EncodedPodRow)> {
	if !scannable(range.start.as_ref(), range.end.as_ref()) {
		return Vec::new();
	}
	bucket.entries
		.range::<EncodedKey, _>((range.start.as_ref(), range.end.as_ref()))
		.filter_map(|(key, row)| row.as_ref().map(|row| (key.clone(), row.clone())))
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

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
	let mut out = prefix[..=last].to_vec();
	out[last] += 1;
	Some(out)
}
