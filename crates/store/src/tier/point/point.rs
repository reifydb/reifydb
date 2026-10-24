// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

use crate::tier::point::{
	Entry, PointDomain, PointKey, PointTier, Shard, account, bucket_hash, entry_footprint, entry_hash,
	find_position,
};

impl<D: PointDomain> PointTier<D> {
	pub fn get(&self, dimension: D::Dimension, key: &D::Key) -> Option<D::Row> {
		let bucket = D::metric_bucket(key)?;
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

	pub fn overwrite(&self, dimension: D::Dimension, key: D::Key, row: D::Row) {
		let Some(bucket) = D::metric_bucket(&key) else {
			return;
		};
		let mut shard = self.shard_at(dimension, &key).lock();
		let id = PointKey {
			dimension,
			key,
		};
		insert_entry(&mut shard, bucket, id, row);
	}

	pub fn invalidate(&self, dimension: D::Dimension, key: &D::Key) {
		if D::metric_bucket(key).is_none() {
			return;
		}
		let hash = bucket_hash(&dimension, key);
		let mut shard = self.shard_at(dimension, key).lock();
		let Some(position) = find_position(&shard, hash, dimension, key) else {
			return;
		};
		shard.remove_at(position as usize);
	}

	pub fn invalidate_dimensions_where(&self, victim: impl Fn(&D::Dimension) -> bool) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
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
}

fn insert_entry<D: PointDomain>(shard: &mut Shard<D>, bucket: usize, id: PointKey<D::Dimension, D::Key>, row: D::Row) {
	let next = shard.next_tick;
	let hash = bucket_hash(&id.dimension, &id.key);
	match find_position(shard, hash, id.dimension, &id.key) {
		Some(position) => {
			let entry = &mut shard.entries[position as usize];
			let old = entry_footprint::<D>(&entry.key, &entry.row);
			if !D::supersede(&mut entry.row, row) {
				return;
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
