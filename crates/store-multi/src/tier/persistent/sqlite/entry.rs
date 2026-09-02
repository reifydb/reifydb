// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{
	catalog::storage::StorageId,
	store::{EntryKind, EntryLayout},
};

const CURRENT_SUFFIX: &str = "__current";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SqliteSchema {
	Blob,
	Row,
	Partitioned,
}

impl SqliteSchema {
	pub(super) fn key_column_count(self) -> usize {
		match self {
			SqliteSchema::Blob | SqliteSchema::Row => 1,
			SqliteSchema::Partitioned => 3,
		}
	}
}

pub(super) fn sqlite_schema(table: EntryKind) -> SqliteSchema {
	match table {
		EntryKind::Source(_, EntryLayout::Row) => SqliteSchema::Row,
		EntryKind::PartitionedSource(_, EntryLayout::Row) => SqliteSchema::Partitioned,
		EntryKind::Multi
		| EntryKind::Source(_, EntryLayout::Series)
		| EntryKind::PartitionedSource(_, EntryLayout::Series) => SqliteSchema::Blob,
	}
}

pub(super) fn entry_id_to_name(kind: EntryKind) -> String {
	match kind {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id, layout) => {
			format!("source_{}_{}_{}", layout.type_tag(), id.type_tag(), id.as_u64())
		}
		EntryKind::PartitionedSource(id, layout) => {
			format!("partsource_{}_{}_{}", layout.type_tag(), id.type_tag(), id.as_u64())
		}
	}
}

pub(super) fn name_to_entry_id(name: &str) -> Option<EntryKind> {
	if name == "multi" {
		return Some(EntryKind::Multi);
	}
	if let Some(rest) = name.strip_prefix("source_") {
		let (layout, tag, id) = split_layout_tag_and_id(rest)?;
		return StorageId::from_type_tag(tag, id).map(|storage| EntryKind::Source(storage, layout));
	}
	if let Some(rest) = name.strip_prefix("partsource_") {
		let (layout, tag, id) = split_layout_tag_and_id(rest)?;
		return StorageId::from_type_tag(tag, id).map(|storage| EntryKind::PartitionedSource(storage, layout));
	}
	None
}

fn split_layout_tag_and_id(rest: &str) -> Option<(EntryLayout, u8, u64)> {
	let (layout, rest) = rest.split_once('_')?;
	let (tag, id) = rest.split_once('_')?;
	Some((EntryLayout::from_type_tag(layout.parse().ok()?)?, tag.parse().ok()?, id.parse().ok()?))
}

pub(super) fn current_table_name(kind: EntryKind) -> String {
	format!("{}{}", entry_id_to_name(kind), CURRENT_SUFFIX)
}

pub(super) fn current_table_name_to_entry(name: &str) -> Option<EntryKind> {
	name.strip_suffix(CURRENT_SUFFIX).and_then(name_to_entry_id)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::{RingBufferId, TableId, ViewId};

	use super::*;

	#[test]
	fn every_constructible_entry_kind_survives_the_name_round_trip() {
		// The persistent tier enumerates its tables by name, so a kind whose name cannot be parsed back becomes
		// invisible to every maintenance pass that walks them.
		let mut covered = 0;
		for tag in 0..=u8::MAX {
			for id in [0u64, 1, 16_391, u64::MAX] {
				let Some(storage) = StorageId::from_type_tag(tag, id) else {
					continue;
				};
				for kind in [
					EntryKind::Source(storage, EntryLayout::Row),
					EntryKind::Source(storage, EntryLayout::Series),
					EntryKind::PartitionedSource(storage, EntryLayout::Row),
					EntryKind::PartitionedSource(storage, EntryLayout::Series),
				] {
					assert_eq!(
						current_table_name_to_entry(&current_table_name(kind)),
						Some(kind),
						"{kind:?} at type tag {tag} did not survive the round trip"
					);
					covered += 1;
				}
			}
		}

		assert_eq!(current_table_name_to_entry(&current_table_name(EntryKind::Multi)), Some(EntryKind::Multi));
		assert!(covered > 0, "the sweep constructed no kinds at all, so it proved nothing about any of them");
	}

	#[test]
	fn a_view_entry_round_trips_on_both_the_source_and_the_partitioned_branch() {
		// A view owns its rows, so it must name its own physical tables under tag 0x02 on both branches.
		let storage = StorageId::view(ViewId(42));
		let row = EntryKind::Source(storage, EntryLayout::Row);
		let partitioned = EntryKind::PartitionedSource(storage, EntryLayout::Row);

		assert_eq!(current_table_name(row), "source_1_2_42__current");
		assert_eq!(current_table_name(partitioned), "partsource_1_2_42__current");

		assert_eq!(
			current_table_name_to_entry("source_1_2_42__current"),
			Some(row),
			"a view's row table must parse back to the view, not to the table of the same id"
		);
		assert_eq!(
			current_table_name_to_entry("partsource_1_2_42__current"),
			Some(partitioned),
			"a view's partitioned row table must parse back to the view, not to the table of the same id"
		);
	}

	#[test]
	fn a_view_names_its_rows_and_its_series_rows_apart() {
		// A view is statically one storage kind, but nothing stops two layouts naming one physical table
		// unless the layout is part of the name, and then a series row would land in a narrow row table.
		let storage = StorageId::view(ViewId(42));

		assert_ne!(
			current_table_name(EntryKind::Source(storage, EntryLayout::Row)),
			current_table_name(EntryKind::Source(storage, EntryLayout::Series))
		);
		assert_ne!(
			current_table_name(EntryKind::PartitionedSource(storage, EntryLayout::Row)),
			current_table_name(EntryKind::PartitionedSource(storage, EntryLayout::Series))
		);
	}

	#[test]
	fn only_a_row_layout_reaches_the_narrow_sqlite_schemas() {
		// The narrow schemas store the row number as a native integer column, which a series key has no room
		// in, so admitting one would silently truncate its sequence.
		let storage = StorageId::view(ViewId(42));

		assert_eq!(sqlite_schema(EntryKind::Source(storage, EntryLayout::Row)), SqliteSchema::Row);
		assert_eq!(
			sqlite_schema(EntryKind::PartitionedSource(storage, EntryLayout::Row)),
			SqliteSchema::Partitioned
		);
		assert_eq!(sqlite_schema(EntryKind::Source(storage, EntryLayout::Series)), SqliteSchema::Blob);
		assert_eq!(
			sqlite_schema(EntryKind::PartitionedSource(storage, EntryLayout::Series)),
			SqliteSchema::Blob
		);
		assert_eq!(sqlite_schema(EntryKind::Multi), SqliteSchema::Blob);
	}

	#[test]
	fn a_table_and_a_view_at_the_same_id_get_distinct_partitioned_table_names() {
		// Collapsing the two onto one physical table would mix a view's rows with its old backing table's.
		let table = EntryKind::PartitionedSource(StorageId::table(TableId(42)), EntryLayout::Row);
		let view = EntryKind::PartitionedSource(StorageId::view(ViewId(42)), EntryLayout::Row);

		assert_ne!(current_table_name(table), current_table_name(view));
	}

	#[test]
	fn two_storage_variants_at_the_same_id_get_distinct_table_names() {
		// Rendering a storage id without its variant collapses distinct objects onto one table, so their rows
		// and tombstones would share a keyspace.
		let table = EntryKind::Source(StorageId::table(TableId(5)), EntryLayout::Row);
		let ringbuffer = EntryKind::Source(StorageId::ringbuffer(RingBufferId(5)), EntryLayout::Row);

		assert_ne!(
			current_table_name(table),
			current_table_name(ringbuffer),
			"a table and a ring buffer sharing id 5 must not share a persistent table"
		);
	}

	#[test]
	fn a_name_the_formatter_never_produces_is_rejected_rather_than_guessed() {
		// A foreign table in the same database must not resolve to a real kind, or maintenance would attribute
		// its rows to an object that does not own them.
		assert_eq!(current_table_name_to_entry("sqlite_sequence"), None);
		assert_eq!(current_table_name_to_entry("source_1_1_5"), None);
		assert_eq!(current_table_name_to_entry("source_1_1__current"), None);
		assert_eq!(current_table_name_to_entry("source_1_0_5__current"), None);
		assert_eq!(current_table_name_to_entry("source_1_notatag_5__current"), None);
		assert_eq!(current_table_name_to_entry("partsource_1_0_5__current"), None);
		assert_eq!(
			current_table_name_to_entry("source_0_2_5__current"),
			None,
			"no layout carries tag 0, so a name claiming one must not resolve to a real entry"
		);
		assert_eq!(
			current_table_name_to_entry("source_2_5__current"),
			None,
			"the pre layout name shape must not resolve, or two shapes would name one physical table"
		);
		assert_eq!(
			current_table_name_to_entry("partsource_1_3_5__current"),
			None,
			"a virtual table holds no rows, so its tag must never name a partitioned entry"
		);
		assert_eq!(
			current_table_name_to_entry("partsource_1_5_5__current"),
			None,
			"a dictionary holds no rows, so its tag must never name a partitioned entry"
		);
	}
}
