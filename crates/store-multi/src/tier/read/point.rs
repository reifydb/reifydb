// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::atomic::Ordering};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::{EntryKind, classify_key},
};
use reifydb_store::row::page::page_of;
use reifydb_value::util::cowvec::CowVec;
use tracing::instrument;

use crate::tier::{
	VersionedGetResult,
	read::{MultiReadBufferTier, PageEntry, ResidentPage},
};

impl MultiReadBufferTier {
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		match classify_key(key) {
			EntryKind::Operator(_) => self.get_operator(key, version),
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
				return VersionedGetResult::NotFound;
			};
			if entry.version > version {
				return VersionedGetResult::NotFound;
			}
			let result = match &entry.value {
				Some(value) => VersionedGetResult::Value {
					value: value.clone(),
					version: entry.version,
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
		match shard.pages.get_mut(&page_id) {
			Some(page) => {
				if let Some(existing) = page.entries.get(&key)
					&& existing.version > version
				{
					return;
				}
				page.entries.insert(
					key,
					PageEntry {
						version,
						value,
					},
				);
				page.hot = true;
				page.tick = next;
			}
			None => {
				let mut entries = BTreeMap::new();
				entries.insert(
					key,
					PageEntry {
						version,
						value,
					},
				);
				shard.pages.insert(
					page_id,
					ResidentPage {
						entries,
						hot: false,
						tick: next,
						range_complete: false,
					},
				);
			}
		}
		shard.next_tick = next + 1;
		shard.evict_to_capacity();
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		let page_id = page_of(key, self.bucket_shift());
		let mut shard = self.shard_for(&page_id).lock();
		let now_empty = match shard.pages.get_mut(&page_id) {
			Some(page) => {
				page.entries.remove(key);
				page.range_complete = false;
				page.entries.is_empty()
			}
			None => false,
		};
		if now_empty {
			shard.pages.remove(&page_id);
		}
	}

	pub fn clear(&self) {
		for shard in self.inner.shards.iter() {
			let mut shard = shard.lock();
			shard.pages.clear();
			shard.next_tick = 0;
		}
	}

	pub fn set_capacity(&self, resident_pages: usize) {
		let page_cap = (resident_pages / self.inner.shards.len()).max(1);
		for shard in self.inner.shards.iter() {
			let mut shard = shard.lock();
			shard.page_cap = page_cap;
			shard.evict_to_capacity();
		}
	}

	pub fn reconfigure(&self, resident_pages: usize, page_size_rows: u64) {
		let bucket_shift = page_size_rows.max(1).trailing_zeros() as u8;
		let page_cap = (resident_pages / self.inner.shards.len()).max(1);
		self.inner.bucket_shift.store(bucket_shift, Ordering::Relaxed);
		for shard in self.inner.shards.iter() {
			let mut shard = shard.lock();
			shard.page_cap = page_cap;
			shard.pages.clear();
			shard.next_tick = 0;
		}
	}
}
