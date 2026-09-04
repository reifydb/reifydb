// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{
	catalog::storage::StorageId,
	store::{EntryKind, EntryLayout},
};

const CURRENT_SUFFIX: &str = "__current";

pub(super) const SERIES_KEY_COLUMNS: [&str; 3] = ["variant_tag", "key", "sequence"];

pub(super) const PARTITIONED_SERIES_KEY_COLUMNS: [&str; 5] =
	["partition_hi", "partition_lo", "variant_tag", "key", "sequence"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SqliteSchema {
	Blob,
	Row,
	Partitioned,
	Series,
	PartitionedSeries,
}

impl SqliteSchema {
	pub(super) fn key_column_count(self) -> usize {
		match self {
			SqliteSchema::Blob | SqliteSchema::Row => 1,
			SqliteSchema::Partitioned | SqliteSchema::Series => 3,
			SqliteSchema::PartitionedSeries => 5,
		}
	}

	pub(super) fn series_key_columns(self) -> Option<&'static [&'static str]> {
		match self {
			SqliteSchema::Series => Some(&SERIES_KEY_COLUMNS),
			SqliteSchema::PartitionedSeries => Some(&PARTITIONED_SERIES_KEY_COLUMNS),
			SqliteSchema::Blob | SqliteSchema::Row | SqliteSchema::Partitioned => None,
		}
	}
}

pub(super) fn sqlite_schema(table: EntryKind) -> SqliteSchema {
	match table {
		EntryKind::Source(_, EntryLayout::Row) => SqliteSchema::Row,
		EntryKind::PartitionedSource(_, EntryLayout::Row) => SqliteSchema::Partitioned,
		EntryKind::Multi
		| EntryKind::Source(_, EntryLayout::Series | EntryLayout::SortedView)
		| EntryKind::PartitionedSource(_, EntryLayout::Series | EntryLayout::SortedView) => SqliteSchema::Blob,
	}
}

pub(super) fn narrow_series_schema(table: EntryKind) -> Option<SqliteSchema> {
	match table {
		EntryKind::Source(_, EntryLayout::Series) => Some(SqliteSchema::Series),
		EntryKind::PartitionedSource(_, EntryLayout::Series) => Some(SqliteSchema::PartitionedSeries),
		EntryKind::Multi
		| EntryKind::Source(_, EntryLayout::Row | EntryLayout::SortedView)
		| EntryKind::PartitionedSource(_, EntryLayout::Row | EntryLayout::SortedView) => None,
	}
}

fn declared_type<'a>(columns: &'a [(String, String)], name: &str) -> Option<&'a str> {
	columns.iter().find(|(column, _)| column == name).map(|(_, declared)| declared.as_str())
}

pub(super) fn series_schema_from_columns(narrow: SqliteSchema, columns: &[(String, String)]) -> Option<SqliteSchema> {
	let expected = narrow.series_key_columns()?;
	if columns.is_empty() {
		return Some(narrow);
	}
	let narrow_columns_present = expected
		.iter()
		.all(|name| declared_type(columns, name).is_some_and(|ty| ty.eq_ignore_ascii_case("INTEGER")));
	let partitioned_columns_present =
		PARTITIONED_SERIES_KEY_COLUMNS.iter().take(2).any(|name| declared_type(columns, name).is_some());
	if narrow_columns_present && (narrow == SqliteSchema::PartitionedSeries || !partitioned_columns_present) {
		return Some(narrow);
	}
	if declared_type(columns, "variant_tag").is_none()
		&& declared_type(columns, "key").is_some_and(|ty| ty.eq_ignore_ascii_case("BLOB"))
	{
		return Some(SqliteSchema::Blob);
	}
	None
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
	fn the_static_mapping_decides_row_layouts_and_defers_every_series_layout() {
		// sqlite_schema is the static mapping only. A row layout is settled here for good, but a series
		// entry is resolved per table by the probe in resolve_schema, so Blob on these arms is the legacy
		// default this mapping falls back to and not a claim about what a series table actually stores.
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

	fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
		pairs.iter().map(|(name, ty)| ((*name).to_string(), (*ty).to_string())).collect()
	}

	fn blob_columns() -> Vec<(String, String)> {
		cols(&[("key", "BLOB"), ("version", "BLOB"), ("value", "BLOB"), ("updated_at", "INTEGER")])
	}

	fn narrow_series_columns() -> Vec<(String, String)> {
		cols(&[
			("variant_tag", "INTEGER"),
			("key", "INTEGER"),
			("sequence", "INTEGER"),
			("version", "BLOB"),
			("value", "BLOB"),
			("updated_at", "INTEGER"),
		])
	}

	fn narrow_partitioned_series_columns() -> Vec<(String, String)> {
		cols(&[
			("partition_hi", "INTEGER"),
			("partition_lo", "INTEGER"),
			("variant_tag", "INTEGER"),
			("key", "INTEGER"),
			("sequence", "INTEGER"),
			("version", "BLOB"),
			("value", "BLOB"),
			("updated_at", "INTEGER"),
		])
	}

	#[test]
	fn an_existing_blob_series_table_is_still_read_as_a_blob_table() {
		// A narrow table also owns a column literally named `key`, so blob SQL run against a narrow table
		// returns zero rows without erroring. Misreading a legacy blob table as narrow is the loud half of
		// that pair; misreading it the other way is the silent half, and this is the assertion that stops it.
		assert_eq!(series_schema_from_columns(SqliteSchema::Series, &blob_columns()), Some(SqliteSchema::Blob));
		assert_eq!(
			series_schema_from_columns(SqliteSchema::PartitionedSeries, &blob_columns()),
			Some(SqliteSchema::Blob)
		);
	}

	#[test]
	fn a_table_that_does_not_exist_yet_is_created_narrow() {
		// PRAGMA table_info reports no columns for an absent table, and an absent table has no data to
		// preserve, so it is the one case where the narrow schema can be chosen unconditionally.
		assert_eq!(series_schema_from_columns(SqliteSchema::Series, &[]), Some(SqliteSchema::Series));
		assert_eq!(
			series_schema_from_columns(SqliteSchema::PartitionedSeries, &[]),
			Some(SqliteSchema::PartitionedSeries)
		);
	}

	#[test]
	fn an_existing_narrow_table_keeps_its_narrow_schema() {
		assert_eq!(
			series_schema_from_columns(SqliteSchema::Series, &narrow_series_columns()),
			Some(SqliteSchema::Series)
		);
		assert_eq!(
			series_schema_from_columns(
				SqliteSchema::PartitionedSeries,
				&narrow_partitioned_series_columns()
			),
			Some(SqliteSchema::PartitionedSeries)
		);
	}

	#[test]
	fn a_partitioned_table_is_never_mistaken_for_an_unpartitioned_one() {
		// Both carry variant_tag/key/sequence. Reading a partitioned table under the three column schema
		// would drop the partition from every bound and mix every partition's rows into one scan.
		assert_eq!(
			series_schema_from_columns(SqliteSchema::Series, &narrow_partitioned_series_columns()),
			None,
			"a partitioned table must not resolve to the unpartitioned narrow schema"
		);
		assert_eq!(
			series_schema_from_columns(SqliteSchema::PartitionedSeries, &narrow_series_columns()),
			None,
			"an unpartitioned table must not resolve to the partitioned narrow schema"
		);
	}

	#[test]
	fn a_layout_the_probe_does_not_recognise_is_refused_rather_than_guessed() {
		// Falling back to Blob here would make every read of an unknown-but-narrow table return zero rows
		// silently. The probe must hand the caller an error instead of a guess.
		assert_eq!(series_schema_from_columns(SqliteSchema::Series, &cols(&[("key", "INTEGER")])), None);
		assert_eq!(
			series_schema_from_columns(
				SqliteSchema::Series,
				&cols(&[("variant_tag", "INTEGER"), ("key", "INTEGER")])
			),
			None,
			"a half narrow table is missing the sequence column and must not be read as narrow"
		);
		assert_eq!(series_schema_from_columns(SqliteSchema::Series, &cols(&[("what", "TEXT")])), None);
	}

	#[test]
	fn only_a_series_entry_kind_asks_the_probe_anything() {
		// The row schemas are decided statically; routing them through the probe would make a working path
		// depend on a PRAGMA that can fail.
		let storage = StorageId::view(ViewId(42));
		assert_eq!(narrow_series_schema(EntryKind::Source(storage, EntryLayout::Row)), None);
		assert_eq!(narrow_series_schema(EntryKind::PartitionedSource(storage, EntryLayout::Row)), None);
		assert_eq!(narrow_series_schema(EntryKind::Multi), None);
		assert_eq!(
			narrow_series_schema(EntryKind::Source(storage, EntryLayout::Series)),
			Some(SqliteSchema::Series)
		);
		assert_eq!(
			narrow_series_schema(EntryKind::PartitionedSource(storage, EntryLayout::Series)),
			Some(SqliteSchema::PartitionedSeries)
		);
	}

	#[test]
	fn every_narrow_schema_reports_as_many_columns_as_it_names() {
		// push_key_params, read_returned_key and the get_many row offsets all index by this count, so a
		// count that disagrees with the column list reads the version out of a key column.
		assert_eq!(SqliteSchema::Series.key_column_count(), SERIES_KEY_COLUMNS.len());
		assert_eq!(SqliteSchema::PartitionedSeries.key_column_count(), PARTITIONED_SERIES_KEY_COLUMNS.len());
		assert_eq!(
			SqliteSchema::Series.series_key_columns().map(<[&str]>::len),
			Some(SqliteSchema::Series.key_column_count())
		);
		assert_eq!(
			SqliteSchema::PartitionedSeries.series_key_columns().map(<[&str]>::len),
			Some(SqliteSchema::PartitionedSeries.key_column_count())
		);
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
