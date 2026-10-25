// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

use crate::tier::point::{
	Entry, PointDomain, PointKey, PointTier, Shard, account, bucket_hash, entry_footprint, entry_hash,
	find_position,
};

const OVERWRITE_HOLD_KEYS: usize = 256;

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

	pub fn overwrite(&self, dimension: D::Dimension, rows: Vec<(D::Key, D::Row)>) {
		let mut placed: Vec<(usize, usize, D::Key, D::Row)> = rows
			.into_iter()
			.filter_map(|(key, row)| {
				let bucket = D::metric_bucket(&key)?;
				Some((self.shard_index(dimension, &key), bucket, key, row))
			})
			.collect();
		placed.sort_by_key(|(index, ..)| *index);
		let mut placed = placed.into_iter().peekable();
		while let Some((index, bucket, key, row)) = placed.next() {
			let mut shard = self.shard(index).lock();
			insert_entry(
				&mut shard,
				bucket,
				PointKey {
					dimension,
					key,
				},
				row,
			);
			let mut held = 1;
			while let Some((_, bucket, key, row)) = placed.next_if(|(next, ..)| *next == index) {
				if held == OVERWRITE_HOLD_KEYS {
					drop(shard);
					shard = self.shard(index).lock();
					held = 0;
				}
				held += 1;
				insert_entry(
					&mut shard,
					bucket,
					PointKey {
						dimension,
						key,
					},
					row,
				);
			}
		}
	}

	pub fn invalidate(&self, dimension: D::Dimension, keys: &[D::Key]) {
		let mut placed: Vec<(usize, &D::Key)> = keys
			.iter()
			.filter(|key| D::metric_bucket(key).is_some())
			.map(|key| (self.shard_index(dimension, key), key))
			.collect();
		placed.sort_by_key(|(index, _)| *index);
		let mut placed = placed.into_iter().peekable();
		while let Some((index, key)) = placed.next() {
			let mut shard = self.shard(index).lock();
			remove_key(&mut shard, dimension, key);
			while let Some((_, key)) = placed.next_if(|(next, _)| *next == index) {
				remove_key(&mut shard, dimension, key);
			}
		}
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

fn remove_key<D: PointDomain>(shard: &mut Shard<D>, dimension: D::Dimension, key: &D::Key) {
	let hash = bucket_hash(&dimension, key);
	if let Some(position) = find_position(shard, hash, dimension, key) {
		shard.remove_at(position as usize);
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
