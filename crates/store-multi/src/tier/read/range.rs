// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::interface::store::EntryKind;
use reifydb_store::row::page::{PageId, key_range_of, page_of};
use tracing::instrument;

use crate::{
	MultiVersionScope,
	tier::{
		RangeBatch, RangeCursor, RawEntry,
		read::{MultiReadBufferTier, PageEntry, ResidentPage, ServedChunk},
	},
};

impl MultiReadBufferTier {
	pub fn page_of_key(&self, key: &EncodedKey) -> PageId {
		page_of(key, self.bucket_shift())
	}

	pub fn page_key_range(&self, page: PageId) -> Option<EncodedKeyRange> {
		key_range_of(page, self.bucket_shift())
	}

	pub fn invalidate_page(&self, page: PageId) {
		let mut shard = self.shard_for(&page).lock();
		shard.pages.remove(&page);
	}

	pub fn populate_page(&self, page: PageId, entries: Vec<RawEntry>, complete: bool) {
		let shift = self.bucket_shift();
		let range_complete = complete && key_range_of(page, shift).is_some();
		let mut shard = self.shard_for(&page).lock();
		let next = shard.next_tick;
		let resident = shard.pages.entry(page).or_insert_with(|| ResidentPage {
			entries: BTreeMap::new(),
			hot: false,
			tick: next,
			range_complete: false,
			warm_blocked: false,
		});
		for entry in entries {
			match resident.entries.get(&entry.key) {
				Some(existing) if existing.version > entry.version => continue,
				_ => {
					resident.entries.insert(
						entry.key,
						PageEntry {
							version: entry.version,
							value: entry.value,
						},
					);
				}
			}
		}
		resident.range_complete = range_complete;
		resident.tick = next;
		shard.next_tick = next + 1;
		shard.evict_to_capacity();
	}

	pub fn finish_warm(&self, page: PageId, entries: Vec<RawEntry>) -> bool {
		let shift = self.bucket_shift();
		let range_complete = key_range_of(page, shift).is_some();
		let mut shard = self.shard_for(&page).lock();
		let Some(dirty) = shard.warming.remove(&page) else {
			return false;
		};
		if dirty || !range_complete {
			return false;
		}
		let next = shard.next_tick;
		let resident = shard.pages.entry(page).or_insert_with(|| ResidentPage {
			entries: BTreeMap::new(),
			hot: false,
			tick: next,
			range_complete: false,
			warm_blocked: false,
		});
		for entry in entries {
			match resident.entries.get(&entry.key) {
				Some(existing) if existing.version > entry.version => continue,
				_ => {
					resident.entries.insert(
						entry.key,
						PageEntry {
							version: entry.version,
							value: entry.value,
						},
					);
				}
			}
		}
		resident.range_complete = true;
		resident.tick = next;
		shard.next_tick = next + 1;
		shard.evict_to_capacity();
		true
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "store::multi::read::serve", level = "trace", skip(self, cursor, start, end), fields(table = ?table, descending = descending))]
	pub fn serve_persistent_chunk(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: &[u8],
		end: &[u8],
		scope: MultiVersionScope,
		batch_size: usize,
		descending: bool,
	) -> ServedChunk {
		match table {
			EntryKind::Source(_) => {}
			EntryKind::Operator(_) => {
				return self.serve_operator_chunk(cursor, start, end, scope, batch_size, descending);
			}
			_ => return ServedChunk::Gap,
		}

		let shift = self.bucket_shift();
		let range_lo = EncodedKey::new(start.to_vec());
		let range_hi = EncodedKey::new(end.to_vec());
		if range_lo > range_hi {
			cursor.exhausted = true;
			return ServedChunk::Served(RangeBatch::empty());
		}

		let mut out: Vec<RawEntry> = Vec::new();
		let mut first = true;
		let mut page = match &cursor.last_key {
			Some(last) => page_of(last, shift),
			None if descending => page_of(&range_hi, shift),
			None => page_of(&range_lo, shift),
		};

		loop {
			let Some(page_range) = key_range_of(page, shift) else {
				if out.is_empty() {
					return ServedChunk::Gap;
				}
				return served_chunk(out, cursor, false);
			};
			let (page_start, page_end) = match (page_range.start, page_range.end) {
				(Bound::Included(s), Bound::Included(e)) => (s, e),
				_ => {
					if out.is_empty() {
						return ServedChunk::Gap;
					}
					return served_chunk(out, cursor, false);
				}
			};

			if descending {
				if page_end < range_lo {
					return served_chunk(out, cursor, true);
				}
			} else if page_start > range_hi {
				return served_chunk(out, cursor, true);
			}

			let mut shard = self.shard_for(&page).lock();
			let complete = shard.pages.get(&page).map(|p| p.range_complete).unwrap_or(false);
			if !complete {
				drop(shard);
				if out.is_empty() {
					return ServedChunk::Gap;
				}
				return served_chunk(out, cursor, false);
			}

			let tick = shard.next_tick;
			let page_ref = shard.pages.get_mut(&page).expect("complete page present under lock");

			let lo_bound: Bound<EncodedKey> = if first {
				match &cursor.last_key {
					Some(last) if !descending && *last >= range_lo => Bound::Excluded(last.clone()),
					_ => Bound::Included(page_start.clone().max(range_lo.clone())),
				}
			} else {
				Bound::Included(page_start.clone().max(range_lo.clone()))
			};
			let hi_bound: Bound<EncodedKey> = if first {
				match &cursor.last_key {
					Some(last) if descending && *last <= range_hi => Bound::Excluded(last.clone()),
					_ => Bound::Included(page_end.clone().min(range_hi.clone())),
				}
			} else {
				Bound::Included(page_end.clone().min(range_hi.clone()))
			};

			let mut full = false;
			if descending {
				for (key, entry) in page_ref.entries.range((lo_bound, hi_bound)).rev() {
					if out.len() >= batch_size {
						full = true;
						break;
					}
					if scope.contains(entry.version) {
						out.push(RawEntry {
							key: key.clone(),
							version: entry.version,
							value: entry.value.clone(),
						});
					}
				}
			} else {
				for (key, entry) in page_ref.entries.range((lo_bound, hi_bound)) {
					if out.len() >= batch_size {
						full = true;
						break;
					}
					if scope.contains(entry.version) {
						out.push(RawEntry {
							key: key.clone(),
							version: entry.version,
							value: entry.value.clone(),
						});
					}
				}
			}

			page_ref.hot = true;
			page_ref.tick = tick;
			shard.next_tick = tick + 1;
			drop(shard);

			if full {
				return served_chunk(out, cursor, false);
			}

			if descending {
				if page_start <= range_lo {
					return served_chunk(out, cursor, true);
				}
				page = PageId {
					kind: page.kind,
					bucket: page.bucket + 1,
				};
			} else {
				if page_end >= range_hi {
					return served_chunk(out, cursor, true);
				}
				if page.bucket == 0 {
					return served_chunk(out, cursor, true);
				}
				page = PageId {
					kind: page.kind,
					bucket: page.bucket - 1,
				};
			}
			first = false;
		}
	}

	fn serve_operator_chunk(
		&self,
		cursor: &mut RangeCursor,
		start: &[u8],
		end: &[u8],
		scope: MultiVersionScope,
		batch_size: usize,
		descending: bool,
	) -> ServedChunk {
		let shift = self.bucket_shift();
		let range_lo = EncodedKey::new(start.to_vec());
		let range_hi = EncodedKey::new(end.to_vec());
		if range_lo > range_hi {
			cursor.exhausted = true;
			return ServedChunk::Served(RangeBatch::empty());
		}

		let page = page_of(&range_lo, shift);
		let Some(page_range) = key_range_of(page, shift) else {
			return ServedChunk::Gap;
		};
		let (Bound::Included(page_start), Bound::Included(page_end)) = (page_range.start, page_range.end)
		else {
			return ServedChunk::Gap;
		};
		if range_lo < page_start || range_hi > page_end {
			return ServedChunk::Gap;
		}

		let mut shard = self.shard_for(&page).lock();
		let complete = shard.pages.get(&page).map(|p| p.range_complete).unwrap_or(false);
		if !complete {
			return ServedChunk::Gap;
		}

		let tick = shard.next_tick;
		let page_ref = shard.pages.get_mut(&page).expect("complete page present under lock");

		let lo_bound: Bound<EncodedKey> = match &cursor.last_key {
			Some(last) if !descending && *last >= range_lo => Bound::Excluded(last.clone()),
			_ => Bound::Included(range_lo.clone()),
		};
		let hi_bound: Bound<EncodedKey> = match &cursor.last_key {
			Some(last) if descending && *last <= range_hi => Bound::Excluded(last.clone()),
			_ => Bound::Included(range_hi.clone()),
		};

		let mut out: Vec<RawEntry> = Vec::new();
		let mut full = false;
		if descending {
			for (key, entry) in page_ref.entries.range((lo_bound, hi_bound)).rev() {
				if out.len() >= batch_size {
					full = true;
					break;
				}
				if entry.version > scope.read() {
					return ServedChunk::Gap;
				}
				if scope.contains(entry.version) {
					out.push(RawEntry {
						key: key.clone(),
						version: entry.version,
						value: entry.value.clone(),
					});
				}
			}
		} else {
			for (key, entry) in page_ref.entries.range((lo_bound, hi_bound)) {
				if out.len() >= batch_size {
					full = true;
					break;
				}
				if entry.version > scope.read() {
					return ServedChunk::Gap;
				}
				if scope.contains(entry.version) {
					out.push(RawEntry {
						key: key.clone(),
						version: entry.version,
						value: entry.value.clone(),
					});
				}
			}
		}

		page_ref.hot = true;
		page_ref.tick = tick;
		shard.next_tick = tick + 1;
		drop(shard);

		served_chunk(out, cursor, !full)
	}

	pub fn page_is_complete(&self, page: PageId) -> bool {
		let shard = self.shard_for(&page).lock();
		shard.pages.get(&page).map(|p| p.range_complete).unwrap_or(false)
	}
}

fn served_chunk(out: Vec<RawEntry>, cursor: &mut RangeCursor, exhausted: bool) -> ServedChunk {
	if let Some(last) = out.last() {
		cursor.last_key = Some(last.key.clone());
	}
	cursor.exhausted = exhausted;
	ServedChunk::Served(RangeBatch {
		entries: out,
		has_more: !exhausted,
	})
}
