// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::interface::store::EntryKind;
use reifydb_store::row::page::{PageId, key_range_of, page_of};
use reifydb_value::reifydb_assertions;
use tracing::instrument;

use crate::{
	MultiVersionScope,
	tier::{
		RangeBatch, RangeCursor, RawEntry,
		read::{
			EntryFootprint, MultiReadBufferTier, PageEntry, ResidentPage, ServedChunk, Shard, account,
			coverage::{span_of, widen},
			entry_footprint,
		},
	},
};

impl MultiReadBufferTier {
	pub fn page_of_key(&self, key: &EncodedKey) -> PageId {
		page_of(key, self.bucket_shift())
	}

	pub fn page_key_range(&self, page: PageId) -> Option<EncodedKeyRange> {
		key_range_of(page, self.bucket_shift())
	}

	pub fn populate_page(&self, page: PageId, entries: Vec<RawEntry>, complete: bool) {
		let shift = self.bucket_shift();
		let span = span_of(key_range_of(page, shift)).filter(|_| complete);
		let index = self.shard_index(&page);
		let token = self.retractions();
		let mut shard = self.shard(index).lock();
		let next = shard.next_tick;
		{
			let Shard {
				pages,
				budget,
				..
			} = &mut *shard;
			let resident = pages.entry(page).or_insert_with(|| ResidentPage::fresh(next));
			for entry in entries {
				let key = entry.key;
				let old = match resident.entries.get(&key) {
					Some(existing) if existing.version > entry.version => continue,
					Some(existing) => entry_footprint(&key, existing),
					None => EntryFootprint::default(),
				};
				let new_entry = PageEntry {
					version: entry.version,
					value: entry.value,
					previous: None,
				};
				let new = entry_footprint(&key, &new_entry);
				resident.entries.insert(key, new_entry);
				account(&mut resident.bytes, &mut resident.payload, budget, old, new);
			}
			resident.tick = next;
			if let Some(span) = span.clone() {
				resident.fills = self.next_fill();
				widen(&mut resident.claimed, span);
			}
		}
		shard.next_tick = next + 1;
		drop(shard);

		if let Some(span) = span {
			self.claim(page.kind, &span, token);
		}
		self.evict_to_capacity(index);
	}

	pub fn finish_warm(&self, page: PageId, entries: Vec<RawEntry>) -> bool {
		let shift = self.bucket_shift();
		let span = span_of(key_range_of(page, shift));
		let index = self.shard_index(&page);
		let mut shard = self.shard(index).lock();
		let Some(claim) = shard.warming.remove(&page) else {
			return false;
		};
		if claim.dirty {
			shard.warm_metrics.warms_dirty_aborted += 1;
			return false;
		}
		let Some(span) = span else {
			shard.warm_metrics.warms_aborted += 1;
			return false;
		};
		let next = shard.next_tick;
		{
			let Shard {
				pages,
				budget,
				..
			} = &mut *shard;
			let resident = pages.entry(page).or_insert_with(|| ResidentPage::fresh(next));
			for entry in entries {
				let key = entry.key;
				let old = match resident.entries.get(&key) {
					Some(existing) if existing.version > entry.version => continue,
					Some(existing) => entry_footprint(&key, existing),
					None => EntryFootprint::default(),
				};
				let new_entry = PageEntry {
					version: entry.version,
					value: entry.value,
					previous: None,
				};
				let new = entry_footprint(&key, &new_entry);
				resident.entries.insert(key, new_entry);
				account(&mut resident.bytes, &mut resident.payload, budget, old, new);
			}
			resident.tick = next;
			resident.fills = self.next_fill();
			widen(&mut resident.claimed, span.clone());
		}
		shard.next_tick = next + 1;
		shard.warm_metrics.warms_completed += 1;
		drop(shard);

		self.claim(page.kind, &span, claim.retractions);
		self.evict_to_capacity(index);
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
			_ => return ServedChunk::Gap,
		}

		let shift = self.bucket_shift();
		let attribution = match &cursor.last_key {
			Some(last) => page_of(last, shift),
			None if descending => page_of(&EncodedKey::new(end), shift),
			None => page_of(&EncodedKey::new(start), shift),
		};

		let chunk = self.serve_covered_chunk(table, cursor, start, end, scope, batch_size, descending);

		{
			let mut shard = self.shard_for(&attribution).lock();
			match &chunk {
				ServedChunk::Served(_) => shard.read_metrics.range_served += 1,
				ServedChunk::Gap => shard.read_metrics.range_gaps += 1,
			}
		}
		chunk
	}
}

pub(super) fn served_chunk(out: Vec<RawEntry>, cursor: &mut RangeCursor, exhausted: bool) -> ServedChunk {
	reifydb_assertions! {
		assert!(
			exhausted || !out.is_empty(),
			"a chunk that reports more must carry an entry, otherwise last_key never advances and the store's scan loop, which now ends only when every tier cursor is exhausted, spins forever"
		);
	}
	if let Some(last) = out.last() {
		cursor.last_key = Some(last.key.clone());
	}
	cursor.exhausted = exhausted;
	ServedChunk::Served(RangeBatch {
		entries: out,
		has_more: !exhausted,
	})
}
