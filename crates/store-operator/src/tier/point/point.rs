// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::Keyspace};
use reifydb_value::byte_size::ByteSize;

use crate::tier::point::{OperatorPointTier, PointKey, Shard, Slot, account, entry_footprint, keyspace_of};

impl OperatorPointTier {
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		let keyspace = keyspace_of(key)?;
		if !keyspace.cache_policy().caches_points() {
			self.charge_excluded_miss(keyspace);
			return None;
		}
		let id = PointKey {
			operator,
			key: key.clone(),
		};
		let counter = keyspace.0 as usize;
		let mut shard = self.shard_for(&id).lock();
		shard.record_access(&id);
		let next = shard.next_tick;
		let Some(position) = shard.index.get(&id).copied() else {
			shard.metrics.misses += 1;
			shard.keyspace_metrics[counter].misses += 1;
			return None;
		};
		let row = {
			let slot = &mut shard.slots[position];
			slot.tick = next;
			slot.row.clone()
		};
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		shard.keyspace_metrics[counter].hits += 1;
		Some(row)
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> Option<bool> {
		let keyspace = keyspace_of(key)?;
		if !keyspace.cache_policy().caches_points() {
			self.charge_excluded_miss(keyspace);
			return None;
		}
		let id = PointKey {
			operator,
			key: key.clone(),
		};
		let counter = keyspace.0 as usize;
		let mut shard = self.shard_for(&id).lock();
		shard.record_access(&id);
		let next = shard.next_tick;
		let Some(position) = shard.index.get(&id).copied() else {
			shard.metrics.misses += 1;
			shard.keyspace_metrics[counter].misses += 1;
			return None;
		};
		let present = {
			let slot = &mut shard.slots[position];
			slot.tick = next;
			slot.row.is_some()
		};
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		shard.keyspace_metrics[counter].hits += 1;
		Some(present)
	}

	pub fn begin_fill(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let Some(keyspace) = keyspace_of(key) else {
			return false;
		};
		if !keyspace.cache_policy().caches_points() {
			return false;
		}
		let id = PointKey {
			operator,
			key: key.clone(),
		};
		let counter = keyspace.0 as usize;
		let mut shard = self.shard_for(&id).lock();
		if shard.filling.contains_key(&id) {
			shard.metrics.fills_duplicate += 1;
			shard.keyspace_metrics[counter].fills_duplicate += 1;
			return false;
		}
		shard.filling.insert(id, false);
		shard.metrics.fills_started += 1;
		shard.keyspace_metrics[counter].fills_started += 1;
		true
	}

	pub fn finish_fill(&self, operator: OperatorId, key: EncodedKey, row: Option<EncodedPodRow>) -> bool {
		let Some(keyspace) = keyspace_of(&key) else {
			return false;
		};
		let id = PointKey {
			operator,
			key,
		};
		let mut shard = self.shard_for(&id).lock();
		match shard.filling.remove(&id) {
			Some(false) => {}
			Some(true) | None => {
				shard.metrics.fills_dirty_aborted += 1;
				shard.keyspace_metrics[keyspace.0 as usize].fills_dirty_aborted += 1;
				return false;
			}
		}
		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, &id);
		}
		insert_entry(&mut shard, keyspace, id, row)
	}

	pub fn abort_fill(&self, operator: OperatorId, key: &EncodedKey) {
		if keyspace_of(key).is_none() {
			return;
		}
		let id = PointKey {
			operator,
			key: key.clone(),
		};
		self.shard_for(&id).lock().filling.remove(&id);
	}

	pub fn overwrite(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		let Some(keyspace) = keyspace_of(&key) else {
			return;
		};
		let id = PointKey {
			operator,
			key,
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(dirty) = shard.filling.get_mut(&id) {
			*dirty = true;
		}
		insert_entry(&mut shard, keyspace, id, Some(row));
	}

	pub fn invalidate(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(keyspace) = keyspace_of(key) else {
			return;
		};
		if !keyspace.cache_policy().caches_points() {
			return;
		}
		let id = PointKey {
			operator,
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

	pub fn invalidate_operator(&self, operator: OperatorId) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			for (key, dirty) in shard.filling.iter_mut() {
				if key.operator == operator {
					*dirty = true;
				}
			}
			let mut released = 0usize;
			{
				let Shard {
					index,
					slots,
					..
				} = &mut *shard;
				slots.retain(|slot| {
					if slot.key.operator != operator {
						return true;
					}
					released += entry_footprint(&slot.key, &slot.row);
					false
				});
				index.clear();
				for (position, slot) in slots.iter().enumerate() {
					index.insert(slot.key.clone(), position);
				}
			}
			shard.budget.release(ByteSize::from_bytes(released as u64));
		}
	}

	pub fn clear(&self) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			shard.index.clear();
			shard.slots.clear();
			shard.filling.clear();
			shard.next_tick = 0;
			shard.sketch.clear();
			shard.budget.reset();
		}
	}
}

fn insert_entry(shard: &mut Shard, keyspace: Keyspace, id: PointKey, row: Option<EncodedPodRow>) -> bool {
	if !keyspace.cache_policy().caches_points() {
		return false;
	}
	let next = shard.next_tick;
	let new = entry_footprint(&id, &row);
	match shard.index.get(&id).copied() {
		Some(position) => {
			let slot = &mut shard.slots[position];
			let old = entry_footprint(&slot.key, &slot.row);
			slot.row = row;
			slot.tick = next;
			account(&shard.budget, old, new);
		}
		None => {
			let fits = shard.budget.used().as_bytes() + new as u64 <= shard.budget.limit().as_bytes();
			if !fits && !shard.admits(&id) {
				shard.metrics.admissions_refused += 1;
				shard.keyspace_metrics[keyspace.0 as usize].admissions_refused += 1;
				return false;
			}
			shard.slots.push(Slot {
				key: id.clone(),
				row,
				tick: next,
			});
			shard.index.insert(id, shard.slots.len() - 1);
			shard.budget.charge(ByteSize::from_bytes(new as u64));
		}
	}
	shard.metrics.insertions += 1;
	shard.keyspace_metrics[keyspace.0 as usize].insertions += 1;
	shard.next_tick = next + 1;
	shard.evict_to_capacity();
	true
}
