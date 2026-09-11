// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::RefCell,
	collections::{BTreeSet, HashMap, HashSet},
	sync::Arc,
};

use reifydb_cdc::rebuild::changed_objects;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		cdc::Cdc,
	},
};

use crate::progress::tracker::UpstreamPosition;

pub struct StreamRead {
	pub items: Vec<Arc<Cdc>>,
	pub read_to: CommitVersion,
	pub more: bool,
}

#[derive(Default)]
pub struct ReadCache {
	reads: HashMap<CommitVersion, (Vec<Arc<Cdc>>, CommitVersion)>,
}

impl ReadCache {
	pub fn get(&self, from: CommitVersion, up_to: CommitVersion) -> Option<StreamRead> {
		let (items, read_to) = self
			.reads
			.iter()
			.filter(|(start, (_, read_to))| **start == from || (**start < from && from < *read_to))
			.max_by_key(|(_, (_, read_to))| *read_to)
			.map(|(_, read)| read)?;
		Some(StreamRead {
			items: items.iter().filter(|cdc| cdc.version.commit > from).cloned().collect(),
			read_to: *read_to,
			more: *read_to < up_to,
		})
	}

	pub fn insert(&mut self, from: CommitVersion, items: Vec<Arc<Cdc>>, read_to: CommitVersion) {
		self.reads.insert(from, (items, read_to));
	}

	pub fn clear(&mut self) {
		self.reads.clear();
	}
}

pub struct UpstreamRead {
	pub views: HashSet<ObjectId>,
	pub position: Option<UpstreamPosition>,
	pub read: StreamRead,
}

#[derive(Default)]
pub struct ObjectIndex {
	changed: RefCell<HashMap<CommitVersion, BTreeSet<ObjectId>>>,
}

impl ObjectIndex {
	fn touches(&self, cdc: &Cdc, objects: &HashSet<ObjectId>) -> bool {
		self.changed
			.borrow_mut()
			.entry(cdc.version.commit)
			.or_insert_with(|| changed_objects(cdc))
			.iter()
			.any(|object| objects.contains(object))
	}
}

impl UpstreamRead {
	fn view_items<'a>(&'a self, index: &'a ObjectIndex) -> impl Iterator<Item = &'a Arc<Cdc>> {
		self.read.items.iter().filter(move |cdc| index.touches(cdc, &self.views))
	}

	fn complete_through(&self, cursor: CommitVersion, index: &ObjectIndex) -> CommitVersion {
		let mut through = cursor;
		if let Some(position) = self.position
			&& self.read.read_to >= position.last_commit
		{
			through = through.max(position.position);
		}
		if let Some(last) = self.view_items(index).last() {
			through = through.max(CommitVersion(last.version.source.0.saturating_sub(1)));
		}
		through
	}

	pub fn needs_extension(&self, cursor: CommitVersion, index: &ObjectIndex) -> bool {
		if !self.read.more {
			return false;
		}
		let mut stamps =
			self.view_items(index).map(|cdc| cdc.version.source.0).filter(|source| *source > cursor.0);
		match stamps.next() {
			Some(first) => stamps.all(|source| source == first),
			None => false,
		}
	}

	pub fn cursor_after(&self, advance_to: CommitVersion, index: &ObjectIndex) -> CommitVersion {
		self.view_items(index)
			.find(|cdc| cdc.version.source.0 > advance_to.0)
			.map(|cdc| CommitVersion(cdc.version.commit.0 - 1))
			.unwrap_or(self.read.read_to)
	}
}

pub struct UpstreamReads {
	pub reads: HashMap<FlowId, UpstreamRead>,
	pub index: ObjectIndex,
}

impl UpstreamReads {
	pub fn cursors_after(&self, advance_to: CommitVersion) -> HashMap<FlowId, CommitVersion> {
		self.reads
			.iter()
			.map(|(producer, read)| (*producer, read.cursor_after(advance_to, &self.index)))
			.collect()
	}
}

pub struct Merged {
	pub items: Vec<Arc<Cdc>>,
	pub target: CommitVersion,
	pub more: bool,
}

pub fn merge(
	cursor: CommitVersion,
	tables: &StreamRead,
	upstreams: &HashMap<FlowId, UpstreamRead>,
	index: &ObjectIndex,
) -> Merged {
	let gated: HashSet<ObjectId> = upstreams.values().flat_map(|upstream| upstream.views.iter().copied()).collect();
	let target = upstreams
		.values()
		.map(|upstream| upstream.complete_through(cursor, index))
		.fold(tables.read_to, CommitVersion::min);

	let mut ordered: Vec<(u64, bool, CommitVersion, Arc<Cdc>)> = Vec::new();
	for cdc in &tables.items {
		if cdc.version.commit > cursor && cdc.version.commit <= target && !index.touches(cdc, &gated) {
			ordered.push((cdc.version.source.0, true, cdc.version.commit, cdc.clone()));
		}
	}
	for upstream in upstreams.values() {
		for cdc in upstream.view_items(index) {
			if cdc.version.source.0 > cursor.0 && cdc.version.source.0 <= target.0 {
				ordered.push((cdc.version.source.0, false, cdc.version.commit, cdc.clone()));
			}
		}
	}
	ordered.sort_by_key(|(source, table, version, _)| (*source, *table, *version));

	Merged {
		items: ordered.into_iter().map(|(_, _, _, cdc)| cdc).collect(),
		target,
		more: tables.more || upstreams.values().any(|upstream| upstream.read.more),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		common::{ChangeVersion, SourceVersion},
		interface::{
			catalog::{id::ViewId, storage::StorageId},
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
			items,
			read_to: cv(read_to),
			more,
		}
	}

	fn upstream(position: Option<(u64, u64)>, read: StreamRead) -> UpstreamRead {
		upstream_of(ViewId(5), position, read)
	}

	fn upstream_of(view: ViewId, position: Option<(u64, u64)>, read: StreamRead) -> UpstreamRead {
		UpstreamRead {
			views: HashSet::from([ObjectId::View(view)]),
			position: position.map(|(position, last_commit)| UpstreamPosition {
				position: cv(position),
				last_commit: cv(last_commit),
			}),
			read,
		}
	}

	fn one(upstream: UpstreamRead) -> HashMap<FlowId, UpstreamRead> {
		HashMap::from([(PRODUCER, upstream)])
	}

	fn two(first: UpstreamRead, second: UpstreamRead) -> HashMap<FlowId, UpstreamRead> {
		HashMap::from([(PRODUCER, first), (OTHER_PRODUCER, second)])
	}

	fn versions(merged: &Merged) -> Vec<(u64, u64)> {
		merged.items.iter().map(|cdc| (cdc.version.commit.0, cdc.version.source.0)).collect()
	}

	#[test]
	fn a_producer_position_holds_the_gate_until_its_last_commit_is_read() {
		// Trusting the position before its commit is read would pass a version whose view rows are still
		// unseen.
		let tables = read(vec![], 20, false);

		let unread = merge(
			cv(0),
			&tables,
			&one(upstream(Some((10, 12)), read(vec![], 11, false))),
			&ObjectIndex::default(),
		);
		assert_eq!(unread.target, cv(0), "the producer commit at 12 is not read yet, so nothing may pass");

		let read_through = merge(
			cv(0),
			&tables,
			&one(upstream(Some((10, 12)), read(vec![], 12, false))),
			&ObjectIndex::default(),
		);
		assert_eq!(read_through.target, cv(10));
	}

	#[test]
	fn an_upstream_with_no_position_holds_the_reader_at_its_cursor() {
		// Without a published position the producer may still emit rows for any source above the cursor.
		let merged = merge(
			cv(3),
			&read(vec![table_row(4)], 20, false),
			&one(upstream(None, read(vec![], 20, false))),
			&ObjectIndex::default(),
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
			&one(upstream(Some((5, 7)), read(vec![view_row(7, 5)], 20, false))),
			&ObjectIndex::default(),
		);
		assert_eq!(versions(&merged), vec![(7, 5), (5, 5)]);
	}

	#[test]
	fn rows_are_ordered_by_source_not_by_commit_version() {
		// Commit order would run the header at 4 before the view row made from the rung at 3.
		let merged = merge(
			cv(0),
			&read(vec![table_row(4)], 20, false),
			&one(upstream(Some((4, 6)), read(vec![view_row(6, 3)], 20, false))),
			&ObjectIndex::default(),
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
			&ObjectIndex::default(),
		);
		assert_eq!(merged.target, cv(5));
		assert_eq!(versions(&merged), vec![(8, 3)], "the row stamped 6 may have siblings beyond the read");
	}

	#[test]
	fn a_truncated_read_whose_rows_share_one_stamp_must_extend() {
		// Without extending, the gate stops one below that stamp and the reader never moves again.
		let cursor = cv(2);
		assert!(upstream(None, read(vec![view_row(8, 4), view_row(9, 4)], 9, true))
			.needs_extension(cursor, &ObjectIndex::default()));
		assert!(!upstream(None, read(vec![view_row(8, 4), view_row(9, 5)], 9, true))
			.needs_extension(cursor, &ObjectIndex::default()));
		assert!(!upstream(None, read(vec![view_row(8, 4)], 9, false))
			.needs_extension(cursor, &ObjectIndex::default()));
		assert!(!upstream(None, read(vec![], 9, true)).needs_extension(cursor, &ObjectIndex::default()));
	}

	#[test]
	fn the_read_position_stops_just_below_the_first_unhandled_view_row() {
		// A position past an unhandled row skips it forever; one at the start re-reads the whole range each
		// step.
		let up = upstream(None, read(vec![view_row(8, 3), view_row(9, 6)], 12, false));
		assert_eq!(up.cursor_after(cv(4), &ObjectIndex::default()), cv(8));
		assert_eq!(up.cursor_after(cv(6), &ObjectIndex::default()), cv(12));
	}

	#[test]
	fn a_view_row_inside_the_table_range_is_merged_once() {
		// The table stream spans the same versions as the view stream, so without exclusion the row runs twice.
		let view = view_row(7, 5);
		let merged = merge(
			cv(0),
			&read(vec![table_row(5), view.clone()], 20, false),
			&one(upstream(Some((5, 7)), read(vec![view], 20, false))),
			&ObjectIndex::default(),
		);
		assert_eq!(versions(&merged), vec![(7, 5), (5, 5)]);
	}

	#[test]
	fn a_view_row_stamped_at_or_below_the_cursor_is_not_handled_again() {
		// After a restart the view stream re-reads from the cursor and would replay rows already handled.
		let merged = merge(
			cv(5),
			&read(vec![], 20, false),
			&one(upstream(Some((9, 9)), read(vec![view_row(7, 5), view_row(9, 8)], 20, false))),
			&ObjectIndex::default(),
		);
		assert_eq!(versions(&merged), vec![(9, 8)]);
	}

	#[test]
	fn the_target_never_passes_a_truncated_table_read() {
		// A table commit beyond the read could carry a source the merge has not seen.
		let merged = merge(
			cv(0),
			&read(vec![table_row(3)], 3, true),
			&one(upstream(Some((10, 10)), read(vec![], 20, false))),
			&ObjectIndex::default(),
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
				upstream(Some((10, 12)), read(vec![view_row(12, 7)], 20, false)),
				upstream_of(ViewId(6), Some((4, 6)), read(vec![], 20, false)),
			),
			&ObjectIndex::default(),
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
				upstream(Some((6, 8)), read(vec![view_row(8, 5)], 20, false)),
				upstream_of(
					ViewId(6),
					Some((6, 9)),
					read(vec![shared, other_view_row(9, 6)], 20, false),
				),
			),
			&ObjectIndex::default(),
		);
		assert_eq!(merged.target, cv(6));
		assert_eq!(
			versions(&merged),
			vec![(6, 5), (8, 5), (5, 5), (9, 6)],
			"a row of either gated view in the table stream must be dropped, not merged a second time"
		);
	}
}
