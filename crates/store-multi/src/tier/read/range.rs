// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{interface::store::EntryKind, util::budget::MemoryBudget};
use reifydb_store::{
	coverage::{Edge, Interval, successor},
	row::page::{PageId, key_range_of, page_of},
};
use reifydb_value::reifydb_assertions;
use tracing::instrument;

use crate::{
	MultiVersionScope,
	tier::{
		RangeBatch, RangeCursor, RawEntry,
		read::{
			EntryFootprint, MultiReadBufferTier, PageEntry, ResidentPage, ServedChunk, Shard, Span,
			account,
			coverage::{span_of, widen},
			entry_footprint,
			scan::page_bounds,
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
			place_entries(resident, budget, entries.into_iter());
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

	pub fn install_scanned_chunk(
		&self,
		table: EntryKind,
		lo: &EncodedKey,
		through: &EncodedKey,
		entries: &[RawEntry],
	) -> bool {
		if !matches!(table, EntryKind::Source(_)) {
			return false;
		}
		let shift = self.bucket_shift();
		let tail = Edge::Key(successor(through));
		if entries.is_empty() {
			let page = page_of(lo, shift);
			if page.kind != table {
				return false;
			}
			return self.install_page_segment(page, shift, lo, tail, entries);
		}

		let mut published = false;
		let mut run = 0;
		while run < entries.len() {
			let page = page_of(&entries[run].key, shift);
			let mut next = run + 1;
			while next < entries.len() && page_of(&entries[next].key, shift) == page {
				next += 1;
			}
			let limit = if next == entries.len() {
				tail.clone()
			} else {
				Edge::Top
			};
			if page.kind == table {
				published |= self.install_page_segment(page, shift, lo, limit, &entries[run..next]);
			}
			run = next;
		}
		published
	}

	fn install_page_segment(
		&self,
		page: PageId,
		shift: u8,
		lo: &EncodedKey,
		limit: Edge,
		entries: &[RawEntry],
	) -> bool {
		let Some((page_start, page_end)) = page_bounds(page, shift) else {
			return false;
		};
		let start = if lo.as_slice() < page_start.as_slice() {
			page_start
		} else {
			lo.clone()
		};
		let end = limit.min(Edge::Key(successor(&page_end)));
		if !end.covers(&start) {
			return false;
		}
		self.place_claim(page, &(start, end), entries)
	}

	fn place_claim(&self, page: PageId, span: &Span, entries: &[RawEntry]) -> bool {
		let index = self.shard_index(&page);
		let token = self.retractions();
		let claimed = Interval::new(span.0.clone(), span.1.clone());
		let placed;
		{
			let mut shard = self.shard(index).lock();
			let next = shard.next_tick;
			{
				let Shard {
					pages,
					budget,
					..
				} = &mut *shard;
				let resident = pages.entry(page).or_insert_with(|| ResidentPage::fresh(next));
				placed = place_entries(
					resident,
					budget,
					entries.iter().filter(|entry| claimed.contains(&entry.key)).cloned(),
				);
				resident.tick = next;
				resident.fills = self.next_fill();
				widen(&mut resident.claimed, span.clone());
			}
			shard.next_tick = next + 1;
		}

		#[cfg(test)]
		self.interlock(page);

		let published = self.claim(page.kind, span, token);
		{
			let mut shard = self.shard(index).lock();
			if published {
				shard.coverage_metrics.installs += 1;
				shard.coverage_metrics.install_rows += placed;
			} else {
				shard.coverage_metrics.installs_refused += 1;
			}
		}
		self.evict_to_capacity(index);
		published
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

fn place_entries(resident: &mut ResidentPage, budget: &MemoryBudget, entries: impl Iterator<Item = RawEntry>) -> u64 {
	let mut placed = 0;
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
		placed += 1;
	}
	placed
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
