// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

use crate::tier::point::{
	Entry, PointDomain, PointKey, PointTier, Shard, account, bucket_hash, entry_footprint, entry_hash,
	find_position,
};

impl<D: PointDomain> PointTier<D> {
	pub fn get(&self, dimension: D::Dimension, key: &D::Key) -> Option<Option<D::Row>> {
		let bucket = D::metric_bucket(key)?;
		if !D::caches_points(bucket) {
			self.charge_excluded_miss(bucket);
			return None;
		}
		let hash = bucket_hash(&dimension, key);
		let mut shard = self.shard_at(dimension, key).lock();
		let next = shard.next_tick;
		let Some(position) = find_position(&shard, hash, dimension, key) else {
			shard.metrics.misses += 1;
			shard.bucket_metrics[bucket].misses += 1;
			return None;
		};
		let row = {
			let entry = &mut shard.entries[position as usize];
			entry.tick = next;
			entry.row.clone()
		};
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		shard.bucket_metrics[bucket].hits += 1;
		Some(row)
	}

	pub fn contains(&self, dimension: D::Dimension, key: &D::Key) -> Option<bool> {
		let bucket = D::metric_bucket(key)?;
		if !D::caches_points(bucket) {
			self.charge_excluded_miss(bucket);
			return None;
		}
		let hash = bucket_hash(&dimension, key);
		let mut shard = self.shard_at(dimension, key).lock();
		let next = shard.next_tick;
		let Some(position) = find_position(&shard, hash, dimension, key) else {
			shard.metrics.misses += 1;
			shard.bucket_metrics[bucket].misses += 1;
			return None;
		};
		let present = {
			let entry = &mut shard.entries[position as usize];
			entry.tick = next;
			entry.row.is_some()
		};
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		shard.bucket_metrics[bucket].hits += 1;
		Some(present)
	}

	pub fn begin_fill(&self, dimension: D::Dimension, key: &D::Key) -> bool {
		let Some(bucket) = D::metric_bucket(key) else {
			return false;
		};
		if !D::caches_points(bucket) {
			return false;
		}
		let mut shard = self.shard_at(dimension, key).lock();
		let id = PointKey {
			dimension,
			key: key.clone(),
		};
		if shard.filling.contains_key(&id) {
			shard.metrics.fills_duplicate += 1;
			shard.bucket_metrics[bucket].fills_duplicate += 1;
			return false;
		}
		shard.filling.insert(id, false);
		shard.metrics.fills_started += 1;
		shard.bucket_metrics[bucket].fills_started += 1;
		true
	}

	pub fn finish_fill(&self, dimension: D::Dimension, key: D::Key, row: Option<D::Row>) -> bool {
		let Some(bucket) = D::metric_bucket(&key) else {
			return false;
		};
		let mut shard = self.shard_at(dimension, &key).lock();
		let id = PointKey {
			dimension,
			key,
		};
		match shard.filling.remove(&id) {
			Some(false) => {}
			Some(true) | None => {
				shard.metrics.fills_dirty_aborted += 1;
				shard.bucket_metrics[bucket].fills_dirty_aborted += 1;
				return false;
			}
		}
		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, &id);
		}
		insert_entry(&mut shard, bucket, id, row);
		true
	}

	pub fn abort_fill(&self, dimension: D::Dimension, key: &D::Key) {
		if D::metric_bucket(key).is_none() {
			return;
		}
		let mut shard = self.shard_at(dimension, key).lock();
		if shard.filling.is_empty() {
			return;
		}
		shard.filling.remove(&PointKey {
			dimension,
			key: key.clone(),
		});
	}

	pub fn overwrite(&self, dimension: D::Dimension, key: D::Key, row: D::Row) {
		let Some(bucket) = D::metric_bucket(&key) else {
			return;
		};
		let mut shard = self.shard_at(dimension, &key).lock();
		let id = PointKey {
			dimension,
			key,
		};
		if let Some(dirty) = shard.filling.get_mut(&id) {
			*dirty = true;
		}
		insert_entry(&mut shard, bucket, id, Some(row));
	}

	pub fn invalidate(&self, dimension: D::Dimension, key: &D::Key) {
		let Some(bucket) = D::metric_bucket(key) else {
			return;
		};
		if !D::caches_points(bucket) {
			return;
		}
		let hash = bucket_hash(&dimension, key);
		let mut shard = self.shard_at(dimension, key).lock();
		if !shard.filling.is_empty() {
			let id = PointKey {
				dimension,
				key: key.clone(),
			};
			if let Some(dirty) = shard.filling.get_mut(&id) {
				*dirty = true;
			}
		}
		let Some(position) = find_position(&shard, hash, dimension, key) else {
			return;
		};
		shard.remove_at(position as usize);
	}

	pub fn invalidate_operator(&self, dimension: D::Dimension) {
		self.invalidate_dimensions_where(|candidate| *candidate == dimension)
	}

	pub fn invalidate_dimensions_where(&self, victim: impl Fn(&D::Dimension) -> bool) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			for (key, dirty) in shard.filling.iter_mut() {
				if victim(&key.dimension) {
					*dirty = true;
				}
			}
			let mut released = 0usize;
			{
				let Shard {
					index,
					entries,
					..
				} = &mut *shard;
				entries.retain(|entry| {
					if !victim(&entry.key.dimension) {
						return true;
					}
					released += entry_footprint::<D>(&entry.key, &entry.row);
					false
				});
				index.clear();
				for position in 0..entries.len() {
					let hash = entry_hash::<D>(&entries[position]);
					index.insert_unique(hash, position as u32, |resident| {
						entry_hash::<D>(&entries[*resident as usize])
					});
				}
			}
			shard.budget.release(ByteSize::from_bytes(released as u64));
		}
	}

	pub fn clear(&self) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			shard.index.clear();
			shard.entries.clear();
			shard.filling.clear();
			shard.next_tick = 0;
			shard.budget.reset();
		}
	}
}

fn insert_entry<D: PointDomain>(
	shard: &mut Shard<D>,
	bucket: usize,
	id: PointKey<D::Dimension, D::Key>,
	row: Option<D::Row>,
) {
	if !D::caches_points(bucket) {
		return;
	}
	let next = shard.next_tick;
	let hash = bucket_hash(&id.dimension, &id.key);
	match find_position(shard, hash, id.dimension, &id.key) {
		Some(position) => {
			let entry = &mut shard.entries[position as usize];
			let old = entry_footprint::<D>(&entry.key, &entry.row);
			match (entry.row.as_mut(), row) {
				(Some(resident), Some(incoming)) => {
					if !D::supersede(resident, incoming) {
						return;
					}
				}
				(_, incoming) => entry.row = incoming,
			}
			entry.tick = next;
			let new = entry_footprint::<D>(&entry.key, &entry.row);
			account(&shard.budget, old, new);
		}
		None => {
			let new = entry_footprint::<D>(&id, &row);
			let Shard {
				index,
				entries,
				budget,
				..
			} = &mut *shard;
			entries.push(Entry {
				key: id,
				row,
				tick: next,
			});
			let position = (entries.len() - 1) as u32;
			index.insert_unique(hash, position, |resident| entry_hash::<D>(&entries[*resident as usize]));
			budget.charge(ByteSize::from_bytes(new as u64));
		}
	}
	shard.metrics.insertions += 1;
	shard.bucket_metrics[bucket].insertions += 1;
	shard.next_tick = next + 1;
	shard.evict_to_capacity();
}
