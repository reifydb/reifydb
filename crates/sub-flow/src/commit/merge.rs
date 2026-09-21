// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	iter::Peekable,
	ops::Deref,
	sync::Arc,
};

use reifydb_cdc::rebuild::changed_objects;
use reifydb_core::{
	common::{CommitVersion, SourceVersion},
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		cdc::Cdc,
	},
};
use reifydb_value::reifydb_assertions;
use rustc_hash::FxHashSet;

use crate::commit::slice::accepts;

#[derive(Clone)]
pub struct ReadWindow {
	chunk: Arc<[Arc<Cdc>]>,
	start: usize,
	end: usize,
}

impl ReadWindow {
	fn slice(&self, start: usize, end: usize) -> Self {
		Self {
			chunk: self.chunk.clone(),
			start: self.start + start,
			end: self.start + end,
		}
	}
}

impl Deref for ReadWindow {
	type Target = [Arc<Cdc>];

	fn deref(&self) -> &[Arc<Cdc>] {
		&self.chunk[self.start..self.end]
	}
}

impl From<Vec<Arc<Cdc>>> for ReadWindow {
	fn from(items: Vec<Arc<Cdc>>) -> Self {
		let end = items.len();
		Self {
			chunk: items.into(),
			start: 0,
			end,
		}
	}
}

pub struct StreamRead {
	pub items: ReadWindow,
	pub read_to: CommitVersion,
	pub more: bool,
}

const READ_CACHE_ENTRIES: usize = 6;

struct CachedRead {
	items: ReadWindow,
	read_to: CommitVersion,
	used: u64,
}

#[derive(Default)]
pub struct ReadCache {
	reads: HashMap<CommitVersion, CachedRead>,
	clock: u64,
}

impl ReadCache {
	pub fn get(&mut self, from: CommitVersion, up_to: CommitVersion) -> Option<StreamRead> {
		self.clock += 1;
		let clock = self.clock;
		let read = self
			.reads
			.iter_mut()
			.filter(|(start, read)| **start <= from && from < read.read_to)
			.max_by_key(|(_, read)| read.read_to)
			.map(|(_, read)| read)?;
		read.used = clock;
		let read_to = read.read_to.min(up_to);
		let start = read.items.partition_point(|cdc| cdc.version.commit <= from);
		let end = read.items.partition_point(|cdc| cdc.version.commit <= read_to);
		Some(StreamRead {
			items: read.items.slice(start, end),
			read_to,
			more: read_to < up_to,
		})
	}

	pub fn insert(&mut self, from: CommitVersion, items: Vec<Arc<Cdc>>, read_to: CommitVersion) -> ReadWindow {
		let items = ReadWindow::from(items);
		if read_to <= from {
			return items;
		}
		self.clock += 1;
		self.reads.insert(
			from,
			CachedRead {
				items: items.clone(),
				read_to,
				used: self.clock,
			},
		);
		while self.reads.len() > READ_CACHE_ENTRIES {
			let Some(oldest) = self.reads.iter().min_by_key(|(_, read)| read.used).map(|(start, _)| *start)
			else {
				break;
			};
			self.reads.remove(&oldest);
		}
		items
	}

	pub fn retain_after(&mut self, cursor: CommitVersion) {
		self.reads.retain(|_, read| read.read_to > cursor);
	}
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadStream {
	Tables,
	Upstream(FlowId),
}

#[derive(Default)]
pub struct HeldReads {
	reads: HashMap<ReadStream, (CommitVersion, StreamRead)>,
}

impl HeldReads {
	pub fn take(&mut self, stream: ReadStream, from: CommitVersion, up_to: CommitVersion) -> Option<StreamRead> {
		let (held_from, mut read) = self.reads.remove(&stream)?;
		if held_from != from {
			return None;
		}
		read.more = read.read_to < up_to;
		Some(read)
	}

	pub fn hold(&mut self, stream: ReadStream, from: CommitVersion, read: StreamRead) {
		self.reads.insert(stream, (from, read));
	}

	pub fn clear(&mut self) {
		self.reads.clear();
	}
}

pub struct UpstreamRead {
	pub views: FxHashSet<ObjectId>,
	pub position: Option<CommitVersion>,
	pub read: StreamRead,
}

fn touches(cdc: &Cdc, objects: &FxHashSet<ObjectId>) -> bool {
	changed_objects(cdc).iter().any(|object| objects.contains(object))
}

fn reads(cdc: &Cdc, source_objects: &BTreeSet<ObjectId>) -> bool {
	changed_objects(cdc).iter().any(|object| accepts(*object, source_objects))
}

impl UpstreamRead {
	fn view_items(&self) -> impl DoubleEndedIterator<Item = &Arc<Cdc>> {
		self.read.items.iter().filter(move |cdc| touches(cdc, &self.views))
	}

	fn complete_through(&self, cursor: CommitVersion) -> CommitVersion {
		let mut through = cursor;
		if let Some(position) = self.position {
			through = through.max(position);
		}
		if let Some(last) = self.view_items().next_back() {
			through = through.max(CommitVersion(last.version.source.0.saturating_sub(1)));
		}
		through
	}

	pub fn needs_extension(&self, cursor: CommitVersion) -> bool {
		if !self.read.more {
			return false;
		}
		let mut stamps = self.view_items().map(|cdc| cdc.version.source.0).filter(|source| *source > cursor.0);
		match stamps.next() {
			Some(first) => stamps.all(|source| source == first),
			None => false,
		}
	}

	pub fn cursor_after(&self, advance_to: CommitVersion) -> CommitVersion {
		self.view_items()
			.find(|cdc| cdc.version.source.0 > advance_to.0)
			.map(|cdc| CommitVersion(cdc.version.commit.0 - 1))
			.unwrap_or(self.read.read_to)
	}
}

pub struct UpstreamReads {
	pub reads: HashMap<FlowId, UpstreamRead>,
}

impl UpstreamReads {
	pub fn cursors_after(&self, advance_to: CommitVersion) -> HashMap<FlowId, CommitVersion> {
		self.reads.iter().map(|(producer, read)| (*producer, read.cursor_after(advance_to))).collect()
	}
}

pub struct Merged {
	pub items: Vec<Arc<Cdc>>,
	pub target: CommitVersion,
	pub more: bool,
}

pub struct StepCut<'a> {
	pub source_objects: &'a BTreeSet<ObjectId>,
	pub per_source: bool,
}

type Rows<'a> = Peekable<Box<dyn Iterator<Item = (bool, &'a Arc<Cdc>)> + 'a>>;

pub fn merge(
	cursor: CommitVersion,
	tables: &StreamRead,
	upstreams: &HashMap<FlowId, UpstreamRead>,
	cut: &StepCut,
) -> Merged {
	let gated: FxHashSet<ObjectId> =
		upstreams.values().flat_map(|upstream| upstream.views.iter().copied()).collect();
	let target = upstreams
		.values()
		.map(|upstream| upstream.complete_through(cursor))
		.fold(tables.read_to, CommitVersion::min);
	let streams = || {
		let table_rows = tables
			.items
			.iter()
			.take_while(|cdc| cdc.version.commit <= target)
			.filter(|cdc| {
				cdc.version.commit > cursor && !touches(cdc, &gated) && reads(cdc, cut.source_objects)
			})
			.map(|cdc| (true, cdc));
		let mut streams: Vec<Rows> = vec![(Box::new(table_rows) as Box<dyn Iterator<Item = _>>).peekable()];
		for upstream in upstreams.values() {
			let view_rows = upstream
				.view_items()
				.filter(move |cdc| cdc.version.source.0 > cursor.0 && cdc.version.source.0 <= target.0)
				.map(|cdc| (false, cdc));
			streams.push((Box::new(view_rows) as Box<dyn Iterator<Item = _>>).peekable());
		}
		streams
	};

	reifydb_assertions! {
		for rows in streams() {
			let sources: Vec<SourceVersion> = rows.map(|(_, cdc)| cdc.version.source).collect();
			assert!(
				sources.is_sorted(),
				"source versions went down within one stream {sources:?}; a per-source step would stop \
				 before the lower source and never handle it"
			);
		}
	}

	let mut ordered: Vec<(bool, CommitVersion, Arc<Cdc>)> = Vec::new();
	let mut next: Option<SourceVersion> = None;
	let mut streams = streams();
	if cut.per_source {
		let first = streams.iter_mut().filter_map(|rows| rows.peek().map(|(_, cdc)| cdc.version.source)).min();
		if let Some(first) = first {
			for rows in &mut streams {
				while let Some((table, cdc)) = rows.next_if(|(_, cdc)| cdc.version.source == first) {
					ordered.push((table, cdc.version.commit, cdc.clone()));
				}
				if let Some((_, cdc)) = rows.peek() {
					next =
						Some(next.map_or(cdc.version.source, |next| {
							next.min(cdc.version.source)
						}));
				}
			}
		}
	} else {
		for rows in streams {
			for (table, cdc) in rows {
				ordered.push((table, cdc.version.commit, cdc.clone()));
			}
		}
	}
	ordered.sort_by_key(|(table, version, cdc)| (cdc.version.source, *table, *version));

	Merged {
		items: ordered.into_iter().map(|(_, _, cdc)| cdc).collect(),
		target: next.map_or(target, |next| CommitVersion(next.0 - 1)),
		more: next.is_some() || tables.more || upstreams.values().any(|upstream| upstream.read.more),
	}
}

#[cfg(test)]
mod tests {
	use std::ptr;

	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		common::{ChangeVersion, SourceVersion},
		interface::{
			catalog::{
				id::{TableId, ViewId},
				storage::StorageId,
			},
			cdc::CdcChange,
		},
		key::row::RowKey,
	};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use super::*;

	const PRODUCER: FlowId = FlowId(10);
	const OTHER_PRODUCER: FlowId = FlowId(11);

	fn cv(version: u64) -> CommitVersion {
		CommitVersion(version)
	}

	fn row(storage: StorageId, version: u64, source: u64) -> Arc<Cdc> {
		Arc::new(Cdc::new(
			ChangeVersion {
				commit: cv(version),
				source: SourceVersion(source),
			},
			DateTime::default(),
			vec![CdcChange::Insert {
				key: RowKey::encoded(storage, RowNumber(version)),
				post: EncodedBytes(CowVec::new(vec![0u8; 4])),
			}],
		))
	}

	fn table_row(version: u64) -> Arc<Cdc> {
		row(StorageId::table(1), version, version)
	}

	fn view_row(version: u64, source: u64) -> Arc<Cdc> {
		row(StorageId::view(5), version, source)
	}

	fn other_view_row(version: u64, source: u64) -> Arc<Cdc> {
		row(StorageId::view(6), version, source)
	}

	fn read(items: Vec<Arc<Cdc>>, read_to: u64, more: bool) -> StreamRead {
		StreamRead {
			items: items.into(),
			read_to: cv(read_to),
			more,
		}
	}

	fn upstream(position: Option<u64>, read: StreamRead) -> UpstreamRead {
		upstream_of(ViewId(5), position, read)
	}

	fn upstream_of(view: ViewId, position: Option<u64>, read: StreamRead) -> UpstreamRead {
		UpstreamRead {
			views: FxHashSet::from_iter([ObjectId::View(view)]),
			position: position.map(cv),
			read,
		}
	}

	fn one(upstream: UpstreamRead) -> HashMap<FlowId, UpstreamRead> {
		HashMap::from([(PRODUCER, upstream)])
	}

	fn two(first: UpstreamRead, second: UpstreamRead) -> HashMap<FlowId, UpstreamRead> {
		HashMap::from([(PRODUCER, first), (OTHER_PRODUCER, second)])
	}

	fn sources() -> BTreeSet<ObjectId> {
		BTreeSet::from([ObjectId::Table(TableId(1)), ObjectId::View(ViewId(5)), ObjectId::View(ViewId(6))])
	}

	fn whole(source_objects: &BTreeSet<ObjectId>) -> StepCut<'_> {
		StepCut {
			source_objects,
			per_source: false,
		}
	}

	fn versions(merged: &Merged) -> Vec<(u64, u64)> {
		merged.items.iter().map(|cdc| (cdc.version.commit.0, cdc.version.source.0)).collect()
	}

	#[test]
	fn an_upstream_with_no_position_holds_the_reader_at_its_cursor() {
		// Without a published position the producer may still emit rows for any source above the cursor.
		let merged = merge(
			cv(3),
			&read(vec![table_row(4)], 20, false),
			&one(upstream(None, read(vec![], 20, false))),
			&whole(&sources()),
		);
		assert_eq!(merged.target, cv(3));
		assert!(merged.items.is_empty(), "a held reader must not hand out table rows beyond its cursor");
	}

	#[test]
	fn view_rows_run_before_table_rows_of_the_same_source() {
		// A header in the same commit as its rungs pairs with nothing if the table row goes first.
		let merged = merge(
			cv(0),
			&read(vec![table_row(5)], 20, false),
			&one(upstream(Some(5), read(vec![view_row(7, 5)], 20, false))),
			&whole(&sources()),
		);
		assert_eq!(versions(&merged), vec![(7, 5), (5, 5)]);
	}

	#[test]
	fn rows_are_ordered_by_source_not_by_commit_version() {
		// Commit order would run the header at 4 before the view row made from the rung at 3.
		let merged = merge(
			cv(0),
			&read(vec![table_row(4)], 20, false),
			&one(upstream(Some(4), read(vec![view_row(6, 3)], 20, false))),
			&whole(&sources()),
		);
		assert_eq!(versions(&merged), vec![(6, 3), (4, 4)]);
	}

	#[test]
	fn a_later_stamp_in_a_truncated_read_proves_every_earlier_stamp_was_read() {
		// Stamps never go down per producer, so only the last stamp seen may still have rows beyond the read.
		let merged = merge(
			cv(0),
			&read(vec![], 20, false),
			&one(upstream(None, read(vec![view_row(8, 3), view_row(9, 6)], 9, true))),
			&whole(&sources()),
		);
		assert_eq!(merged.target, cv(5));
		assert_eq!(versions(&merged), vec![(8, 3)], "the row stamped 6 may have siblings beyond the read");
	}

	#[test]
	fn a_truncated_read_whose_rows_share_one_stamp_must_extend() {
		// Without extending, the gate stops one below that stamp and the reader never moves again.
		let cursor = cv(2);
		assert!(upstream(None, read(vec![view_row(8, 4), view_row(9, 4)], 9, true)).needs_extension(cursor));
		assert!(!upstream(None, read(vec![view_row(8, 4), view_row(9, 5)], 9, true)).needs_extension(cursor));
		assert!(!upstream(None, read(vec![view_row(8, 4)], 9, false)).needs_extension(cursor));
		assert!(!upstream(None, read(vec![], 9, true)).needs_extension(cursor));
	}

	#[test]
	fn the_read_position_stops_just_below_the_first_unhandled_view_row() {
		// A position past an unhandled row skips it forever; one at the start re-reads the whole range each
		// step.
		let up = upstream(None, read(vec![view_row(8, 3), view_row(9, 6)], 12, false));
		assert_eq!(up.cursor_after(cv(4)), cv(8));
		assert_eq!(up.cursor_after(cv(6)), cv(12));
	}

	#[test]
	fn a_view_row_inside_the_table_range_is_merged_once() {
		// The table stream spans the same versions as the view stream, so without exclusion the row runs twice.
		let view = view_row(7, 5);
		let merged = merge(
			cv(0),
			&read(vec![table_row(5), view.clone()], 20, false),
			&one(upstream(Some(5), read(vec![view], 20, false))),
			&whole(&sources()),
		);
		assert_eq!(versions(&merged), vec![(7, 5), (5, 5)]);
	}

	#[test]
	fn a_view_row_stamped_at_or_below_the_cursor_is_not_handled_again() {
		// After a restart the view stream re-reads from the cursor and would replay rows already handled.
		let merged = merge(
			cv(5),
			&read(vec![], 20, false),
			&one(upstream(Some(9), read(vec![view_row(7, 5), view_row(9, 8)], 20, false))),
			&whole(&sources()),
		);
		assert_eq!(versions(&merged), vec![(9, 8)]);
	}

	#[test]
	fn the_target_never_passes_a_truncated_table_read() {
		// A table commit beyond the read could carry a source the merge has not seen.
		let merged = merge(
			cv(0),
			&read(vec![table_row(3)], 3, true),
			&one(upstream(Some(10), read(vec![], 20, false))),
			&whole(&sources()),
		);
		assert_eq!(merged.target, cv(3));
		assert!(merged.more);
	}

	#[test]
	fn the_slower_of_two_producers_holds_the_gate() {
		// Passing the slower producer's position would hand out a source whose view rows from it are still
		// unwritten.
		let merged = merge(
			cv(0),
			&read(vec![], 20, false),
			&two(
				upstream(Some(10), read(vec![view_row(12, 7)], 20, false)),
				upstream_of(ViewId(6), Some(4), read(vec![], 20, false)),
			),
			&whole(&sources()),
		);
		assert_eq!(merged.target, cv(4));
		assert!(
			merged.items.is_empty(),
			"the faster producer's row stamped 7 must wait for the slower producer"
		);
	}

	#[test]
	fn rows_of_two_producers_at_one_source_run_together_before_the_table_row() {
		// A join over both views misses a side if one producer's row at a source runs after the table row of
		// that source.
		let shared = other_view_row(6, 5);
		let merged = merge(
			cv(0),
			&read(vec![table_row(5), shared.clone()], 20, false),
			&two(
				upstream(Some(6), read(vec![view_row(8, 5)], 20, false)),
				upstream_of(ViewId(6), Some(6), read(vec![shared, other_view_row(9, 6)], 20, false)),
			),
			&whole(&sources()),
		);
		assert_eq!(merged.target, cv(6));
		assert_eq!(
			versions(&merged),
			vec![(6, 5), (8, 5), (5, 5), (9, 6)],
			"a row of either gated view in the table stream must be dropped, not merged a second time"
		);
	}

	fn per_source(source_objects: &BTreeSet<ObjectId>) -> StepCut<'_> {
		StepCut {
			source_objects,
			per_source: true,
		}
	}

	#[test]
	fn a_per_source_step_hands_out_one_source_and_stops_below_the_next() {
		// Handing out the whole tail makes every drain rework rows the step drops at its first source change.
		let objects = sources();
		let merged = merge(
			cv(0),
			&read(vec![table_row(5), table_row(6), table_row(7)], 20, false),
			&one(upstream(Some(10), read(vec![view_row(8, 5), view_row(9, 7)], 20, false))),
			&per_source(&objects),
		);
		assert_eq!(versions(&merged), vec![(8, 5), (5, 5)]);
		assert_eq!(merged.target, cv(5), "the step must stop just below source 6, the lowest one left");
		assert!(merged.more, "sources 6 and 7 are still unhandled");
	}

	#[test]
	fn the_lowest_source_of_any_stream_runs_first() {
		// Taking the table stream's first source would run the header at 5 before the view row made from 3.
		let objects = sources();
		let merged = merge(
			cv(0),
			&read(vec![table_row(5)], 20, false),
			&one(upstream(Some(10), read(vec![view_row(8, 3)], 20, false))),
			&per_source(&objects),
		);
		assert_eq!(versions(&merged), vec![(8, 3)]);
		assert_eq!(merged.target, cv(4));
	}

	#[test]
	fn a_row_the_flow_does_not_read_neither_runs_nor_cuts_the_step() {
		// Cutting at an unread source turns every foreign commit into a step of its own.
		let objects = sources();
		let merged = merge(
			cv(0),
			&read(vec![table_row(5), row(StorageId::table(2), 6, 6), table_row(7)], 20, false),
			&one(upstream(Some(10), read(vec![], 20, false))),
			&per_source(&objects),
		);
		assert_eq!(versions(&merged), vec![(5, 5)]);
		assert_eq!(merged.target, cv(6), "the unread commit 6 must be passed together with source 5");
	}

	#[test]
	fn a_per_source_step_with_one_source_left_runs_to_the_target() {
		// Stopping below a source that does not exist would never let the reader reach the gate.
		let objects = sources();
		let merged = merge(
			cv(0),
			&read(vec![table_row(5)], 20, false),
			&one(upstream(Some(20), read(vec![view_row(8, 5)], 20, false))),
			&per_source(&objects),
		);
		assert_eq!(versions(&merged), vec![(8, 5), (5, 5)]);
		assert_eq!(merged.target, cv(20));
		assert!(!merged.more, "nothing is left beyond the target");
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "source versions went down")]
	fn a_stream_whose_sources_go_down_is_rejected() {
		// A per-source step stops before the lower source, so its rows would never run.
		let objects = sources();
		merge(
			cv(0),
			&read(vec![row(StorageId::table(1), 5, 5), row(StorageId::table(1), 6, 3)], 20, false),
			&one(upstream(Some(10), read(vec![], 20, false))),
			&per_source(&objects),
		);
	}

	fn chunk(from: u64, to: u64) -> Vec<Arc<Cdc>> {
		(from + 1..=to).map(table_row).collect()
	}

	fn commits(read: &StreamRead) -> Vec<u64> {
		read.items.iter().map(|cdc| cdc.version.commit.0).collect()
	}

	#[test]
	fn a_cached_chunk_serves_a_later_cursor_cut_at_the_asked_bound() {
		// A later cursor inside a kept chunk must be served from it, never past the bound it asks for.
		let mut cache = ReadCache::default();
		cache.insert(cv(0), chunk(0, 10), cv(10));

		let inside = cache.get(cv(4), cv(7)).expect("cursor 4 lies inside the chunk 0..10");
		assert_eq!(commits(&inside), vec![5, 6, 7]);
		assert_eq!(inside.read_to, cv(7));
		assert!(!inside.more, "the chunk covers the whole asked range");

		let past = cache.get(cv(4), cv(20)).expect("cursor 4 lies inside the chunk 0..10");
		assert_eq!(commits(&past), vec![5, 6, 7, 8, 9, 10]);
		assert_eq!(past.read_to, cv(10));
		assert!(past.more, "versions 11..20 were never read, so the caller must be told to read on");
	}

	#[test]
	fn a_served_read_shares_the_cached_chunk_instead_of_copying_it() {
		// A copy per serve costs the whole unhandled tail on every drain of a lagging flow.
		let mut cache = ReadCache::default();
		let first = cache.insert(cv(0), chunk(0, 10), cv(10));
		let served = cache.get(cv(4), cv(10)).expect("cursor 4 lies inside the chunk 0..10");
		assert!(ptr::eq(&served.items[0], &first[4]), "the served read must point into the cached chunk");
		assert!(ptr::eq(&cache.get(cv(0), cv(10)).expect("chunk 0..10").items[0], &first[0]));
	}

	#[test]
	fn a_cursor_at_a_chunk_end_misses_instead_of_reading_nothing_forever() {
		// Serving an empty read at a chunk end would pin the stream there and the gate would never open.
		let mut cache = ReadCache::default();
		cache.insert(cv(0), chunk(0, 10), cv(10));
		assert!(cache.get(cv(10), cv(20)).is_none(), "nothing past 10 is cached");

		for start in 1..READ_CACHE_ENTRIES as u64 {
			cache.insert(cv(start * 100), chunk(start * 100, start * 100 + 10), cv(start * 100 + 10));
		}
		cache.insert(cv(10), vec![], cv(10));
		assert!(cache.get(cv(10), cv(20)).is_none(), "an empty range must never be served");
		assert!(cache.get(cv(5), cv(10)).is_some(), "an empty range must not take the slot of a real chunk");
	}

	#[test]
	fn a_chunk_the_cursor_has_passed_is_dropped() {
		// Keeping passed chunks would grow the cache with every step of a flow that never reads them again.
		let mut cache = ReadCache::default();
		cache.insert(cv(0), chunk(0, 10), cv(10));
		cache.insert(cv(10), chunk(10, 20), cv(20));

		cache.retain_after(cv(10));

		assert!(cache.get(cv(5), cv(20)).is_none(), "the chunk 0..10 lies wholly at or below the cursor");
		let kept = cache.get(cv(12), cv(20)).expect("the chunk 10..20 is still ahead of the cursor");
		assert_eq!(commits(&kept), (13..=20).collect::<Vec<_>>());
	}

	#[test]
	fn a_full_cache_evicts_the_chunk_used_longest_ago() {
		// Evicting a chunk still in use would send its reader back to storage on every drain.
		let mut cache = ReadCache::default();
		for start in 0..READ_CACHE_ENTRIES as u64 {
			cache.insert(cv(start * 10), chunk(start * 10, start * 10 + 10), cv(start * 10 + 10));
		}
		assert!(cache.get(cv(5), cv(100)).is_some(), "touch the oldest chunk so it becomes the newest");

		let next = READ_CACHE_ENTRIES as u64 * 10;
		cache.insert(cv(next), chunk(next, next + 10), cv(next + 10));

		assert!(cache.get(cv(5), cv(100)).is_some(), "the chunk just used must survive the eviction");
		assert!(cache.get(cv(15), cv(100)).is_none(), "the chunk 10..20 was used longest ago and must go");
	}
}
