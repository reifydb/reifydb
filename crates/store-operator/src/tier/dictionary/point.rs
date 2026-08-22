// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{interface::catalog::flow::OperatorId, util::budget::MemoryBudget};
use reifydb_value::byte_size::ByteSize;

use crate::tier::dictionary::{DictionaryKey, OperatorDictionaryTier, Shard, Slot, entry_footprint};

impl OperatorDictionaryTier {
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		let id = DictionaryKey::of(operator, key)?;
		let mut shard = self.shard_for(&id).lock();
		let next = shard.next_tick;
		let Some(position) = shard.index.get(&id).copied() else {
			shard.metrics.misses += 1;
			return None;
		};
		let slot = &mut shard.slots[position];
		slot.tick = next;
		let row = slot.row.clone();
		shard.next_tick = next + 1;
		shard.metrics.hits += 1;
		Some(row)
	}

	pub fn begin_fill(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let Some(id) = DictionaryKey::of(operator, key) else {
			return false;
		};
		let mut shard = self.shard_for(&id).lock();
		if shard.filling.contains_key(&id) {
			shard.metrics.fills_duplicate += 1;
			return false;
		}
		shard.filling.insert(id, false);
		shard.metrics.fills_started += 1;
		true
	}

	pub fn finish_fill(&self, operator: OperatorId, key: &EncodedKey, row: Option<EncodedPodRow>) -> bool {
		let Some(id) = DictionaryKey::of(operator, key) else {
			return false;
		};
		let mut shard = self.shard_for(&id).lock();
		match shard.filling.remove(&id) {
			Some(false) => {}
			Some(true) | None => {
				shard.metrics.fills_dirty_aborted += 1;
				return false;
			}
		}
		if let Some(row) = row {
			insert_entry(&mut shard, id, row);
		}
		true
	}

	pub fn abort_fill(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(id) = DictionaryKey::of(operator, key) else {
			return;
		};
		self.shard_for(&id).lock().filling.remove(&id);
	}

	pub fn overwrite(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
		let Some(id) = DictionaryKey::of(operator, key) else {
			return;
		};
		let mut shard = self.shard_for(&id).lock();
		if let Some(dirty) = shard.filling.get_mut(&id) {
			*dirty = true;
		}
		insert_entry(&mut shard, id, row);
	}

	pub fn invalidate(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(id) = DictionaryKey::of(operator, key) else {
			return;
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
			shard.budget.reset();
		}
	}
}

fn account(budget: &MemoryBudget, old: usize, new: usize) {
	if new >= old {
		budget.charge(ByteSize::from_bytes((new - old) as u64));
	} else {
		budget.release(ByteSize::from_bytes((old - new) as u64));
	}
}

fn insert_entry(shard: &mut Shard, id: DictionaryKey, row: EncodedPodRow) {
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
	shard.next_tick = next + 1;
	shard.evict_to_capacity();
}
