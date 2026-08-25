// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::{EntryKind, classify_key},
};
use reifydb_store::{
	coverage::{Edge, successor},
	row::page::page_of,
};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};
use tracing::instrument;

use crate::tier::{
	VersionedGetResult,
	read::{
		EntryFootprint, MultiReadBufferTier, PageEntry, ResidentPage, Shard, account, coverage::widen,
		entry_footprint,
	},
};

impl MultiReadBufferTier {
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		match classify_key(key) {
			EntryKind::Source(_) => self.get_source(key, version),
			_ => self.get_multi(key, version),
		}
	}

	#[instrument(name = "store::multi::read::get::source", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_source(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		self.get_impl(key, version)
	}

	#[instrument(name = "store::multi::read::get::multi", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_multi(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		self.get_impl(key, version)
	}

	fn get_impl(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		let page_id = page_of(key, self.bucket_shift());
		let mut shard = self.shard_for(&page_id).lock();
		let next = shard.next_tick;
		let result = {
			let Shard {
				pages,
				read_metrics,
				..
			} = &mut *shard;
			let Some(page) = pages.get_mut(&page_id) else {
				read_metrics.point_misses += 1;
				return VersionedGetResult::NotFound;
			};
			let Some(entry) = page.entries.get(key) else {
				read_metrics.point_misses += 1;
				return VersionedGetResult::NotFound;
			};
			let served = if entry.version <= version {
				read_metrics.point_hits += 1;
				Some((entry.version, entry.value.clone()))
			} else {
				match &entry.previous {
					Some((prev_version, prev_value)) if *prev_version <= version => {
						read_metrics.previous_hits += 1;
						Some((*prev_version, prev_value.clone()))
					}
					_ => None,
				}
			};
			let Some((served_version, served_value)) = served else {
				read_metrics.point_misses += 1;
				return VersionedGetResult::NotFound;
			};
			let result = match served_value {
				Some(value) => VersionedGetResult::Value {
					value,
					version: served_version,
				},
				None => VersionedGetResult::Tombstone,
			};
			page.hot = true;
			page.tick = next;
			result
		};
		shard.next_tick = next + 1;
		result
	}

	pub fn insert(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		let page_id = page_of(&key, self.bucket_shift());
		let index = self.shard_index(&page_id);
		self.lower_head(page_id.kind, &key);
		let token = self.retractions();
		let island = (key.clone(), Edge::Key(successor(&key)));
		let mut shard = self.shard(index).lock();
		let next = shard.next_tick;
		let placed = 'place: {
			let Shard {
				pages,
				budget,
				..
			} = &mut *shard;
			let Some(page) = pages.get_mut(&page_id) else {
				let entry = PageEntry {
					version,
					value,
					previous: None,
				};
				let footprint = entry_footprint(&key, &entry);
				let mut fresh = ResidentPage::fresh(next);
				fresh.entries.insert(key.clone(), entry);
				fresh.bytes = footprint.resident;
				fresh.payload = footprint.payload;
				fresh.fills = self.next_fill();
				fresh.claimed = Some(island.clone());
				budget.charge(ByteSize::from_bytes(footprint.resident as u64));
				pages.insert(page_id, fresh);
				break 'place true;
			};
			match page.entries.get_mut(&key) {
				Some(existing) if existing.version > version => break 'place false,
				Some(existing) if existing.version == version => {
					let old = entry_footprint(&key, existing);
					existing.value = value;
					existing.previous = None;
					let new = entry_footprint(&key, existing);
					account(&mut page.bytes, &mut page.payload, budget, old, new);
				}
				Some(existing) => {
					let old = entry_footprint(&key, existing);
					existing.previous = Some((existing.version, existing.value.take()));
					existing.version = version;
					existing.value = value;
					let new = entry_footprint(&key, existing);
					account(&mut page.bytes, &mut page.payload, budget, old, new);
				}
				None => {
					let entry = PageEntry {
						version,
						value,
						previous: None,
					};
					let footprint = entry_footprint(&key, &entry);
					page.entries.insert(key.clone(), entry);
					account(
						&mut page.bytes,
						&mut page.payload,
						budget,
						EntryFootprint::default(),
						footprint,
					);
				}
			}
			page.hot = true;
			page.tick = next;
			page.fills = self.next_fill();
			widen(&mut page.claimed, island.clone());
			true
		};
		if !placed {
			return;
		}
		shard.next_tick = next + 1;
		drop(shard);

		#[cfg(test)]
		self.interlock(page_id);

		if !self.claims(page_id.kind, &key) {
			self.claim(page_id.kind, &island, token);
		}
		self.evict_to_capacity(index);
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		let page_id = page_of(key, self.bucket_shift());
		let index = self.shard_index(&page_id);
		let was_complete = self.page_is_complete(page_id);
		self.withdraw_key(page_id.kind, key);
		let mut shard = self.shard(index).lock();
		let Shard {
			pages,
			budget,
			page_metrics,
			..
		} = &mut *shard;
		let now_empty = match pages.get_mut(&page_id) {
			Some(page) => {
				if let Some(removed) = page.entries.remove(key) {
					let footprint = entry_footprint(key, &removed);
					account(
						&mut page.bytes,
						&mut page.payload,
						budget,
						footprint,
						EntryFootprint::default(),
					);
				}
				if was_complete {
					page_metrics.complete_pages_invalidated += 1;
				}
				page.entries.is_empty()
			}
			None => false,
		};
		drop(shard);

		#[cfg(test)]
		self.invalidate_interlock(key);

		self.withdraw_key(page_id.kind, key);
		if now_empty {
			self.retract_page(index, page_id);
		}
	}

	pub fn remove_dropped(&self, key: &EncodedKey) {
		let page_id = page_of(key, self.bucket_shift());
		let index = self.shard_index(&page_id);
		self.withdraw_key(page_id.kind, key);
		let mut shard = self.shard(index).lock();
		let Shard {
			pages,
			budget,
			..
		} = &mut *shard;
		let now_empty = match pages.get_mut(&page_id) {
			Some(page) => {
				if let Some(removed) = page.entries.remove(key) {
					let footprint = entry_footprint(key, &removed);
					account(
						&mut page.bytes,
						&mut page.payload,
						budget,
						footprint,
						EntryFootprint::default(),
					);
				}
				page.entries.is_empty()
			}
			None => false,
		};
		drop(shard);
		self.withdraw_key(page_id.kind, key);
		if now_empty {
			self.retract_page(index, page_id);
		}
	}

	pub fn remove_dropped_through(&self, key: &EncodedKey, through: CommitVersion) {
		let page_id = page_of(key, self.bucket_shift());
		let index = self.shard_index(&page_id);
		self.withdraw_key(page_id.kind, key);
		let mut shard = self.shard(index).lock();
		let Shard {
			pages,
			budget,
			..
		} = &mut *shard;
		let now_empty = match pages.get_mut(&page_id) {
			Some(page) => {
				let (do_remove, do_clear_previous) = match page.entries.get(key) {
					Some(entry) if entry.version <= through => (true, false),
					Some(entry) if entry.previous.as_ref().is_some_and(|(v, _)| *v <= through) => {
						(false, true)
					}
					_ => (false, false),
				};
				if do_remove && let Some(removed) = page.entries.remove(key) {
					let footprint = entry_footprint(key, &removed);
					account(
						&mut page.bytes,
						&mut page.payload,
						budget,
						footprint,
						EntryFootprint::default(),
					);
				} else if do_clear_previous && let Some(entry) = page.entries.get_mut(key) {
					let old = entry_footprint(key, entry);
					entry.previous = None;
					let new = entry_footprint(key, entry);
					account(&mut page.bytes, &mut page.payload, budget, old, new);
				}
				page.entries.is_empty()
			}
			None => false,
		};
		drop(shard);
		self.withdraw_key(page_id.kind, key);
		if now_empty {
			self.retract_page(index, page_id);
		}
	}

	pub fn clear(&self) {
		self.withdraw_all();
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			shard.pages.clear();
			shard.next_tick = 0;
			shard.budget.reset();
		}
		self.withdraw_all();
	}
}
