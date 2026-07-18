// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::atomic::Ordering};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::{EntryKind, classify_key},
};
use reifydb_store::row::page::{PageId, page_of};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};
use tracing::instrument;

use crate::tier::{
	VersionedGetResult,
	read::{EntryFootprint, MultiReadBufferTier, PageEntry, ResidentPage, Shard, account, entry_footprint},
};

impl MultiReadBufferTier {
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		match classify_key(key) {
			EntryKind::Operator(_) | EntryKind::OperatorInternal(_) => self.get_operator(key, version),
			EntryKind::Source(_) => self.get_source(key, version),
			_ => self.get_multi(key, version),
		}
	}

	#[instrument(name = "store::multi::read::get::operator", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_operator(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		self.get_impl(key, version)
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
			let Some(page) = shard.pages.get_mut(&page_id) else {
				return VersionedGetResult::NotFound;
			};
			let Some(entry) = page.entries.get(key) else {
				if page.range_complete {
					page.hot = true;
					page.tick = next;
					return VersionedGetResult::Tombstone;
				}
				return VersionedGetResult::NotFound;
			};
			let served = if entry.version <= version {
				Some((entry.version, entry.value.clone()))
			} else {
				match &entry.previous {
					Some((prev_version, prev_value)) if *prev_version <= version => {
						Some((*prev_version, prev_value.clone()))
					}
					_ => None,
				}
			};
			let Some((served_version, served_value)) = served else {
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
		let mut shard = self.shard_for(&page_id).lock();
		let next = shard.next_tick;
		{
			let Shard {
				pages,
				budget,
				..
			} = &mut *shard;
			match pages.get_mut(&page_id) {
				Some(page) => {
					match page.entries.get_mut(&key) {
						Some(existing) if existing.version > version => return,
						Some(existing) if existing.version == version => {
							let old = entry_footprint(&key, existing);
							existing.value = value;
							existing.previous = None;
							let new = entry_footprint(&key, existing);
							account(&mut page.bytes, &mut page.payload, budget, old, new);
						}
						Some(existing) => {
							let old = entry_footprint(&key, existing);
							existing.previous =
								Some((existing.version, existing.value.take()));
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
							page.entries.insert(key, entry);
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
				}
				None => {
					let entry = PageEntry {
						version,
						value,
						previous: None,
					};
					let footprint = entry_footprint(&key, &entry);
					let mut entries = BTreeMap::new();
					entries.insert(key, entry);
					budget.charge(ByteSize::from_bytes(footprint.resident as u64));
					pages.insert(
						page_id,
						ResidentPage {
							entries,
							bytes: footprint.resident,
							payload: footprint.payload,
							hot: false,
							tick: next,
							range_complete: false,
							warm_blocked: false,
						},
					);
				}
			}
		}
		shard.next_tick = next + 1;
		shard.evict_to_capacity();
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		let page_id = page_of(key, self.bucket_shift());
		let mut shard = self.shard_for(&page_id).lock();
		if let Some(dirty) = shard.warming.get_mut(&page_id) {
			*dirty = true;
		}
		let Shard {
			pages,
			budget,
			warm_stats,
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
				if page.range_complete {
					warm_stats.complete_pages_invalidated += 1;
				}
				page.range_complete = false;
				page.entries.is_empty()
			}
			None => false,
		};
		if now_empty {
			pages.remove(&page_id);
		}
	}

	pub fn remove_dropped(&self, key: &EncodedKey) {
		let page_id = page_of(key, self.bucket_shift());
		let mut shard = self.shard_for(&page_id).lock();
		if let Some(dirty) = shard.warming.get_mut(&page_id) {
			*dirty = true;
		}
		let Shard {
			pages,
			budget,
			..
		} = &mut *shard;
		let now_empty_incomplete = match pages.get_mut(&page_id) {
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
				page.entries.is_empty() && !page.range_complete
			}
			None => false,
		};
		if now_empty_incomplete {
			pages.remove(&page_id);
		}
	}

	pub fn remove_dropped_through(&self, key: &EncodedKey, through: CommitVersion) {
		let page_id = page_of(key, self.bucket_shift());
		let mut shard = self.shard_for(&page_id).lock();
		if let Some(dirty) = shard.warming.get_mut(&page_id) {
			*dirty = true;
		}
		let Shard {
			pages,
			budget,
			..
		} = &mut *shard;
		let now_empty_incomplete = match pages.get_mut(&page_id) {
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
				page.entries.is_empty() && !page.range_complete
			}
			None => false,
		};
		if now_empty_incomplete {
			pages.remove(&page_id);
		}
	}

	pub fn page_is_warm_candidate(&self, page: PageId) -> bool {
		let shard = self.shard_for(&page).lock();
		match shard.pages.get(&page) {
			Some(p) => !p.range_complete && !p.warm_blocked,
			None => true,
		}
	}

	pub fn set_warm_blocked(&self, page: PageId) {
		let mut shard = self.shard_for(&page).lock();
		let next = shard.next_tick;
		shard.pages
			.entry(page)
			.or_insert_with(|| ResidentPage {
				entries: BTreeMap::new(),
				bytes: 0,
				payload: 0,
				hot: false,
				tick: next,
				range_complete: false,
				warm_blocked: false,
			})
			.warm_blocked = true;
		shard.warm_stats.pages_warm_blocked += 1;
	}

	pub fn begin_warm(&self, page: PageId) -> bool {
		let mut shard = self.shard_for(&page).lock();
		if shard.warming.contains_key(&page) {
			return false;
		}
		shard.warming.insert(page, false);
		shard.warm_stats.warms_started += 1;
		true
	}

	pub fn abort_warm(&self, page: PageId) {
		let mut shard = self.shard_for(&page).lock();
		if shard.warming.remove(&page).is_some() {
			shard.warm_stats.warms_aborted += 1;
		}
	}

	pub fn clear(&self) {
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			shard.pages.clear();
			shard.warming.clear();
			shard.next_tick = 0;
			shard.budget.reset();
		}
	}

	pub fn set_capacity(&self, resident_pages: usize) {
		for shards in [&self.inner.operator_shards, &self.inner.general_shards] {
			let page_cap = (resident_pages / shards.len()).max(1);
			for shard in shards.iter() {
				let mut shard = shard.lock();
				shard.page_cap = page_cap;
				shard.evict_to_capacity();
			}
		}
	}

	pub fn reconfigure(&self, resident_pages: usize, page_size_rows: u64) {
		let bucket_shift = page_size_rows.max(1).trailing_zeros() as u8;
		self.inner.bucket_shift.store(bucket_shift, Ordering::Relaxed);
		for shards in [&self.inner.operator_shards, &self.inner.general_shards] {
			let page_cap = (resident_pages / shards.len()).max(1);
			for shard in shards.iter() {
				let mut shard = shard.lock();
				shard.page_cap = page_cap;
				shard.pages.clear();
				shard.warming.clear();
				shard.next_tick = 0;
				shard.budget.reset();
			}
		}
	}
}
