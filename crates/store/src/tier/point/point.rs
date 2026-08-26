// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_value::byte_size::ByteSize;

use crate::tier::point::{Entry, PointDomain, PointKey, PointTier, Shard, account, entry_footprint};

impl<D: PointDomain> PointTier<D> {
	pub fn get(&self, dimension: D::Dimension, key: &EncodedKey) -> Option<Option<D::Row>> {
		let slot = D::slot(key)?;
		if !D::caches_points(slot) {
			self.charge_excluded_miss(slot);
			return None;
		}
		let id = PointKey {
			dimension,
			key: key.clone(),
		};
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let Some(position) = shard.index.get(&id).copied() else {
			shard.metrics.misses += 1;
			shard.slot_metrics[slot].misses += 1;
			return None;
		};
		let row = {
			let entry = &mut shard.entries[position];
			entry.tick = next;
			entry.row.clone()
		};
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		shard.slot_metrics[slot].hits += 1;
		Some(row)
	}

	pub fn contains(&self, dimension: D::Dimension, key: &EncodedKey) -> Option<bool> {
		let slot = D::slot(key)?;
		if !D::caches_points(slot) {
			self.charge_excluded_miss(slot);
			return None;
		}
		let id = PointKey {
			dimension,
			key: key.clone(),
		};
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let Some(position) = shard.index.get(&id).copied() else {
			shard.metrics.misses += 1;
			shard.slot_metrics[slot].misses += 1;
			return None;
		};
		let present = {
			let entry = &mut shard.entries[position];
			entry.tick = next;
			entry.row.is_some()
		};
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		shard.slot_metrics[slot].hits += 1;
		Some(present)
	}

	pub fn begin_fill(&self, dimension: D::Dimension, key: &EncodedKey) -> bool {
		let Some(slot) = D::slot(key) else {
			return false;
		};
		if !D::caches_points(slot) {
			return false;
		}
		let id = PointKey {
			dimension,
			key: key.clone(),
		};
		let mut shard = self.shard_for(&id).lock();
		if shard.filling.contains_key(&id) {
			shard.metrics.fills_duplicate += 1;
			shard.slot_metrics[slot].fills_duplicate += 1;
			return false;
		}
		shard.filling.insert(id, false);
		shard.metrics.fills_started += 1;
		shard.slot_metrics[slot].fills_started += 1;
		true
	}

	pub fn finish_fill(&self, dimension: D::Dimension, key: EncodedKey, row: Option<D::Row>) -> bool {
		let Some(slot) = D::slot(&key) else {
			return false;
		};
		let id = PointKey {
			dimension,
			key,
		};
		let mut shard = self.shard_for(&id).lock();
		match shard.filling.remove(&id) {
			Some(false) => {}
			Some(true) | None => {
				shard.metrics.fills_dirty_aborted += 1;
				shard.slot_metrics[slot].fills_dirty_aborted += 1;
				return false;
			}
		}
		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, &id);
		}
		insert_entry(&mut shard, slot, id, row);
		true
	}

	pub fn abort_fill(&self, dimension: D::Dimension, key: &EncodedKey) {
		if D::slot(key).is_none() {
			return;
		}
		let id = PointKey {
			dimension,
			key: key.clone(),
		};
		self.shard_for(&id).lock().filling.remove(&id);
	}

	pub fn overwrite(&self, dimension: D::Dimension, key: EncodedKey, row: D::Row) {
		let Some(slot) = D::slot(&key) else {
			return;
		};
		let id = PointKey {
			dimension,
			key,
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(dirty) = shard.filling.get_mut(&id) {
			*dirty = true;
		}
		insert_entry(&mut shard, slot, id, Some(row));
	}

	pub fn invalidate(&self, dimension: D::Dimension, key: &EncodedKey) {
		let Some(slot) = D::slot(key) else {
			return;
		};
		if !D::caches_points(slot) {
			return;
		}
		let id = PointKey {
			dimension,
			key: key.clone(),
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(dirty) = shard.filling.get_mut(&id) {
			*dirty = true;
		}
		let Some(position) = shard.index.get(&id).copied() else {
			return;
		};
		shard.remove_at(position);
	}

	pub fn invalidate_operator(&self, dimension: D::Dimension) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			for (key, dirty) in shard.filling.iter_mut() {
				if key.dimension == dimension {
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
					if entry.key.dimension != dimension {
						return true;
					}
					released += entry_footprint::<D>(&entry.key, &entry.row);
					false
				});
				index.clear();
				for (position, entry) in entries.iter().enumerate() {
					index.insert(entry.key.clone(), position);
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

fn insert_entry<D: PointDomain>(shard: &mut Shard<D>, slot: usize, id: PointKey<D::Dimension>, row: Option<D::Row>) {
	if !D::caches_points(slot) {
		return;
	}
	let next = shard.next_tick;
	match shard.index.get(&id).copied() {
		Some(position) => {
			let entry = &mut shard.entries[position];
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
			shard.entries.push(Entry {
				key: id.clone(),
				row,
				tick: next,
			});
			shard.index.insert(id, shard.entries.len() - 1);
			shard.budget.charge(ByteSize::from_bytes(new as u64));
		}
	}
	shard.metrics.insertions += 1;
	shard.slot_metrics[slot].insertions += 1;
	shard.next_tick = next + 1;
	shard.evict_to_capacity();
}
