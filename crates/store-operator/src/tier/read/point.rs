// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::read::{BUCKET_OVERHEAD, Bucket, BucketId, OperatorReadBufferTier, Shard, account, entry_footprint};

impl OperatorReadBufferTier {
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		let id = BucketId::of(operator, key)?;
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let result = {
			let Shard {
				buckets,
				metrics,
				..
			} = &mut *shard;
			let Some(bucket) = buckets.get_mut(&id) else {
				metrics.misses += 1;
				return None;
			};
			let cached = bucket.entries.get(key).cloned();
			match cached {
				Some(row) => {
					metrics.hits += 1;
					bucket.tick = next;
					row
				}
				None if bucket.complete => {
					metrics.hits += 1;
					bucket.tick = next;
					None
				}
				None => {
					metrics.misses += 1;
					return None;
				}
			}
		};
		shard.next_tick = next + 1;
		Some(result)
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> Option<bool> {
		let id = BucketId::of(operator, key)?;
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let result = {
			let Shard {
				buckets,
				metrics,
				..
			} = &mut *shard;
			let Some(bucket) = buckets.get_mut(&id) else {
				metrics.misses += 1;
				return None;
			};
			let cached = bucket.entries.get(key).map(Option::is_some);
			match cached {
				Some(present) => {
					metrics.hits += 1;
					bucket.tick = next;
					present
				}
				None if bucket.complete => {
					metrics.hits += 1;
					bucket.tick = next;
					false
				}
				None => {
					metrics.misses += 1;
					return None;
				}
			}
		};
		shard.next_tick = next + 1;
		Some(result)
	}

	pub fn remember(&self, operator: OperatorId, key: EncodedKey, row: Option<EncodedPodRow>) {
		let Some(id) = BucketId::of(operator, &key) else {
			return;
		};
		let mut shard = self.shard_for(&id).lock();
		insert_entry(&mut shard, id, key, row);
	}

	pub fn mark_complete(&self, bucket: BucketId) {
		let mut shard = self.shard_for(&bucket).lock();
		let next = shard.next_tick;
		{
			let Shard {
				buckets,
				budget,
				..
			} = &mut *shard;
			let entry = buckets.entry(bucket).or_insert_with(|| {
				budget.charge(ByteSize::from_bytes(BUCKET_OVERHEAD as u64));
				Bucket {
					entries: BTreeMap::new(),
					bytes: BUCKET_OVERHEAD,
					complete: false,
					tick: next,
				}
			});
			entry.complete = true;
			entry.tick = next;
		}
		shard.next_tick = next + 1;
		shard.evict_to_capacity();
	}

	pub fn is_complete(&self, bucket: BucketId) -> bool {
		self.shard_for(&bucket).lock().buckets.get(&bucket).is_some_and(|found| found.complete)
	}

	pub fn begin_fill(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let Some(id) = BucketId::of(operator, key) else {
			return false;
		};
		let mut shard = self.shard_for(&id).lock();
		if shard.filling.contains_key(&(id, key.clone())) {
			shard.metrics.fills_duplicate += 1;
			return false;
		}
		shard.filling.insert((id, key.clone()), false);
		shard.metrics.fills_started += 1;
		true
	}

	pub fn finish_fill(&self, operator: OperatorId, key: EncodedKey, row: Option<EncodedPodRow>) -> bool {
		let Some(id) = BucketId::of(operator, &key) else {
			return false;
		};
		let mut shard = self.shard_for(&id).lock();
		match shard.filling.remove(&(id, key.clone())) {
			Some(false) => {}
			Some(true) | None => {
				shard.metrics.fills_dirty_aborted += 1;
				return false;
			}
		}
		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, id);
		}
		insert_entry(&mut shard, id, key, row);
		true
	}

	pub fn abort_fill(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(id) = BucketId::of(operator, key) else {
			return;
		};
		self.shard_for(&id).lock().filling.remove(&(id, key.clone()));
	}

	pub fn invalidate(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(id) = BucketId::of(operator, key) else {
			return;
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(dirty) = shard.filling.get_mut(&(id, key.clone())) {
			*dirty = true;
		}
		let Shard {
			buckets,
			budget,
			..
		} = &mut *shard;
		let Some(bucket) = buckets.get_mut(&id) else {
			return;
		};
		if let Some(removed) = bucket.entries.remove(key) {
			let footprint = entry_footprint(key, &removed);
			account(&mut bucket.bytes, budget, footprint, 0);
		}
		bucket.complete = false;
		if bucket.entries.is_empty() {
			let bytes = bucket.bytes;
			buckets.remove(&id);
			budget.release(ByteSize::from_bytes(bytes as u64));
		}
	}

	pub fn invalidate_operator(&self, operator: OperatorId) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			for ((bucket, _), dirty) in shard.filling.iter_mut() {
				if bucket.operator == operator {
					*dirty = true;
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

fn insert_entry(shard: &mut Shard, id: BucketId, key: EncodedKey, row: Option<EncodedPodRow>) {
	let next = shard.next_tick;
	{
		let Shard {
			buckets,
			budget,
			..
		} = &mut *shard;
		match buckets.get_mut(&id) {
			Some(bucket) => {
				let old =
					bucket.entries.get(&key).map_or(0, |previous| entry_footprint(&key, previous));
				let new = entry_footprint(&key, &row);
				bucket.entries.insert(key, row);
				account(&mut bucket.bytes, budget, old, new);
				bucket.tick = next;
			}
			None => {
				let footprint = entry_footprint(&key, &row);
				let mut entries = BTreeMap::new();
				entries.insert(key, row);
				let bytes = BUCKET_OVERHEAD + footprint;
				budget.charge(ByteSize::from_bytes(bytes as u64));
				buckets.insert(
					id,
					Bucket {
						entries,
						bytes,
						complete: false,
						tick: next,
					},
				);
			}
		}
	}
	shard.next_tick = next + 1;
	shard.evict_to_capacity();
}
