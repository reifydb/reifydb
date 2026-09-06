// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::storage::StorageId,
	key::{
		catalog::KeyDeserializerCatalogExt,
		kind::KeyKind,
		row::{PartitionedRowKey, RowKey, StoragePartitionedRowKey, StorageRowKey},
		series::{
			PartitionedSeriesKeyColumns, PartitionedSeriesRowKey, SeriesKeyColumns, SeriesRowKey,
			StoragePartitionedSeriesKey, StorageSeriesKey,
		},
		typed::key::Key,
	},
};
use reifydb_value::value::{partition::Partition, row_number::RowNumber};

use crate::tier::{persistent::sqlite::entry::SqliteSchema, range::NarrowLayout};

pub(super) fn row_ident_of(key: &[u8]) -> Option<StorageRowKey> {
	RowKey::decode(&EncodedKey::new(key)).map(StorageRowKey::from)
}

pub(super) fn row_key_for(storage: StorageId, row: i64) -> EncodedKey {
	RowKey::encoded(storage, RowNumber(row_from_sql(row)))
}

pub(super) fn partitioned_ident_of(key: &[u8]) -> Option<StoragePartitionedRowKey> {
	PartitionedRowKey::decode(&EncodedKey::new(key)).map(StoragePartitionedRowKey::from)
}

pub(super) fn partitioned_key_for(storage: StorageId, partition_hi: i64, partition_lo: i64, row: i64) -> EncodedKey {
	let ident = StoragePartitionedRowKey::from_halves(
		partition_half_from_sql(partition_hi),
		partition_half_from_sql(partition_lo),
		RowNumber(row_from_sql(row)),
	);
	PartitionedRowKey::encoded(storage, ident.partition(), ident.row())
}

fn partition_only_of(key: &[u8]) -> Option<Partition> {
	let mut de = KeyDeserializer::from_bytes(key);
	let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
	if kind != <PartitionedRowKey as Key>::KIND {
		return None;
	}
	de.read_object_id().ok()?;
	let partition = de.read_u128().ok()?;
	if !de.is_empty() {
		return None;
	}
	Some(Partition(partition))
}

pub(super) fn row_to_sql(row: u64) -> i64 {
	((!row) ^ (1u64 << 63)) as i64
}

pub(super) fn row_from_sql(value: i64) -> u64 {
	!((value as u64) ^ (1u64 << 63))
}

pub(super) fn partition_half_to_sql(half: u64) -> i64 {
	row_to_sql(half)
}

pub(super) fn partition_half_from_sql(value: i64) -> u64 {
	row_from_sql(value)
}

fn partition_halves(partition: Partition) -> (i64, i64) {
	(partition_half_to_sql((partition.0 >> 64) as u64), partition_half_to_sql(partition.0 as u64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowBoundary {
	Row(i64),
	Unbounded,
}

fn row_boundary(key: &[u8]) -> RowBoundary {
	match row_ident_of(key) {
		Some(ident) => RowBoundary::Row(row_to_sql(ident.row().0)),
		None => RowBoundary::Unbounded,
	}
}

pub(super) struct RowRangeBounds {
	pub lower: Bound<i64>,
	pub upper: Bound<i64>,
}

fn row_bound(bound: Bound<&[u8]>) -> Bound<i64> {
	match bound {
		Bound::Included(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Included(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Excluded(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Excluded(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Unbounded => Bound::Unbounded,
	}
}

pub(super) fn row_range_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> RowRangeBounds {
	RowRangeBounds {
		lower: row_bound(start),
		upper: row_bound(end),
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionedBoundary {
	Full(i64, i64, i64),
	Unbounded,
}

fn partitioned_boundary(key: &[u8]) -> PartitionedBoundary {
	match partitioned_ident_of(key) {
		Some(ident) => PartitionedBoundary::Full(
			partition_half_to_sql(ident.partition_hi()),
			partition_half_to_sql(ident.partition_lo()),
			row_to_sql(ident.row().0),
		),
		None => PartitionedBoundary::Unbounded,
	}
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let i = prefix.iter().rposition(|&b| b != 0xff)?;
	let mut out = prefix[..i].to_vec();
	out.push(prefix[i] + 1);
	Some(out)
}

fn partition_prefix_and_row(bytes: &[u8]) -> Option<(&[u8], Option<i64>)> {
	if let Some(ident) = partitioned_ident_of(bytes) {
		let split = bytes.len() - 8;
		return Some((&bytes[..split], Some(row_to_sql(ident.row().0))));
	}
	if partition_only_of(bytes).is_some() {
		return Some((bytes, None));
	}
	None
}

pub(super) enum PartitionedRangeBounds {
	ExactPartition {
		partition_hi: i64,
		partition_lo: i64,
		lower_row: Bound<i64>,
		upper_row: Bound<i64>,
	},
	Open {
		lower: Bound<(i64, i64, i64)>,
		upper: Bound<(i64, i64, i64)>,
	},
}

pub(super) fn partitioned_range_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> PartitionedRangeBounds {
	let start_excluded = matches!(start, Bound::Excluded(_));
	let start_bytes = match start {
		Bound::Included(k) | Bound::Excluded(k) => Some(k),
		Bound::Unbounded => None,
	};
	let end_bytes = match end {
		Bound::Included(k) | Bound::Excluded(k) => Some(k),
		Bound::Unbounded => None,
	};

	if let Some(sb) = start_bytes
		&& let Some((prefix, row)) = partition_prefix_and_row(sb)
		&& let Some(eb) = end_bytes
		&& prefix_successor(prefix).as_deref() == Some(eb)
		&& let Some(partition) = partition_only_of(prefix)
	{
		let (partition_hi, partition_lo) = partition_halves(partition);
		let lower_row = match row {
			Some(r) if start_excluded => Bound::Excluded(r),
			Some(r) => Bound::Included(r),
			None => Bound::Unbounded,
		};
		return PartitionedRangeBounds::ExactPartition {
			partition_hi,
			partition_lo,
			lower_row,
			upper_row: Bound::Unbounded,
		};
	}

	let lower = match start_bytes.map(partitioned_boundary) {
		Some(PartitionedBoundary::Full(hi, lo, row)) => {
			if start_excluded {
				Bound::Excluded((hi, lo, row))
			} else {
				Bound::Included((hi, lo, row))
			}
		}
		_ => Bound::Unbounded,
	};
	let end_excluded = matches!(end, Bound::Excluded(_));
	let upper = match end_bytes.map(partitioned_boundary) {
		Some(PartitionedBoundary::Full(hi, lo, row)) => {
			if end_excluded {
				Bound::Excluded((hi, lo, row))
			} else {
				Bound::Included((hi, lo, row))
			}
		}
		_ => Bound::Unbounded,
	};
	PartitionedRangeBounds::Open {
		lower,
		upper,
	}
}

const SERIES_SUFFIX_WIDTHS: [usize; 3] = [2, 8, 8];

const PARTITIONED_SERIES_SUFFIX_WIDTHS: [usize; 5] = [8, 8, 2, 8, 8];

pub(super) fn series_ident_of(key: &[u8]) -> Option<StorageSeriesKey> {
	SeriesRowKey::decode(&EncodedKey::new(key)).map(StorageSeriesKey::from)
}

pub(super) fn partitioned_series_ident_of(key: &[u8]) -> Option<StoragePartitionedSeriesKey> {
	PartitionedSeriesRowKey::decode(&EncodedKey::new(key)).map(StoragePartitionedSeriesKey::from)
}

pub(super) fn series_key_for(storage: StorageId, variant_tag: i64, key: i64, sequence: i64) -> EncodedKey {
	StorageSeriesKey::from_sql_columns(SeriesKeyColumns {
		variant_tag,
		key,
		sequence,
	})
	.with_storage(storage)
	.encode()
}

pub(super) fn partitioned_series_key_for(
	storage: StorageId,
	partition_hi: i64,
	partition_lo: i64,
	variant_tag: i64,
	key: i64,
	sequence: i64,
) -> EncodedKey {
	StoragePartitionedSeriesKey::from_sql_columns(PartitionedSeriesKeyColumns {
		partition_hi,
		partition_lo,
		variant_tag,
		key,
		sequence,
	})
	.with_storage(storage)
	.encode()
}

pub(super) fn series_suffix_widths(schema: SqliteSchema) -> Option<&'static [usize]> {
	match schema {
		SqliteSchema::Series => Some(&SERIES_SUFFIX_WIDTHS),
		SqliteSchema::PartitionedSeries => Some(&PARTITIONED_SERIES_SUFFIX_WIDTHS),
		SqliteSchema::Blob | SqliteSchema::Row | SqliteSchema::Partitioned => None,
	}
}

pub(super) fn series_storage_header(schema: SqliteSchema, storage: StorageId) -> Option<EncodedKey> {
	match schema {
		SqliteSchema::Series => Some(<StorageSeriesKey as NarrowLayout>::storage_start(storage)),
		SqliteSchema::PartitionedSeries => {
			Some(<StoragePartitionedSeriesKey as NarrowLayout>::storage_start(storage))
		}
		SqliteSchema::Blob | SqliteSchema::Row | SqliteSchema::Partitioned => None,
	}
}

fn series_column(field: &[u8]) -> i64 {
	match field.len() {
		2 => u16::from_be_bytes([field[0], field[1]]) as i64,
		_ => {
			let mut buf = [0u8; 8];
			buf.copy_from_slice(field);
			(u64::from_be_bytes(buf) ^ (1u64 << 63)) as i64
		}
	}
}

fn series_columns_of(suffix: &[u8], widths: &[usize]) -> Vec<i64> {
	let mut out = Vec::with_capacity(widths.len());
	let mut at = 0usize;
	for width in widths {
		out.push(series_column(&suffix[at..at + width]));
		at += width;
	}
	out
}

enum SeriesEdge {
	Below,
	Above,
	Columns(Vec<i64>, bool),
	Malformed,
}

fn series_edge(header: &[u8], widths: &[usize], bytes: &[u8]) -> SeriesEdge {
	if !bytes.starts_with(header) {
		return if bytes < header {
			SeriesEdge::Below
		} else {
			SeriesEdge::Above
		};
	}
	let suffix = &bytes[header.len()..];
	let suffix_len: usize = widths.iter().sum();
	if suffix.len() > suffix_len {
		return SeriesEdge::Malformed;
	}
	let mut padded = vec![0u8; suffix_len];
	padded[..suffix.len()].copy_from_slice(suffix);
	SeriesEdge::Columns(series_columns_of(&padded, widths), suffix.len() == suffix_len)
}

pub(super) enum SeriesRangeBounds {
	Empty,
	Range {
		lower: Bound<Vec<i64>>,
		upper: Bound<Vec<i64>>,
	},
}

pub(super) fn series_range_bounds(
	header: &[u8],
	widths: &[usize],
	start: Bound<&[u8]>,
	end: Bound<&[u8]>,
) -> Option<SeriesRangeBounds> {
	let lower = match start {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(bytes) | Bound::Excluded(bytes) => match series_edge(header, widths, bytes) {
			SeriesEdge::Below => Bound::Unbounded,
			SeriesEdge::Above => return Some(SeriesRangeBounds::Empty),
			SeriesEdge::Malformed => return None,
			SeriesEdge::Columns(columns, exact) => {
				if exact && matches!(start, Bound::Excluded(_)) {
					Bound::Excluded(columns)
				} else {
					Bound::Included(columns)
				}
			}
		},
	};

	let upper = match end {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(bytes) | Bound::Excluded(bytes) => match series_edge(header, widths, bytes) {
			SeriesEdge::Above => Bound::Unbounded,
			SeriesEdge::Below => return Some(SeriesRangeBounds::Empty),
			SeriesEdge::Malformed => return None,
			SeriesEdge::Columns(columns, exact) => {
				if exact && matches!(end, Bound::Included(_)) {
					Bound::Included(columns)
				} else {
					Bound::Excluded(columns)
				}
			}
		},
	};

	Some(SeriesRangeBounds::Range {
		lower,
		upper,
	})
}

#[cfg(test)]
mod series_bound_tests {
	use std::ops::RangeBounds;

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::key::{
		any::AnyKey,
		series::{PartitionedSeriesRowKeyRange, SeriesRowKeyRange},
	};
	use reifydb_value::value::{Value, partition::Partition};

	use super::*;

	fn storage() -> StorageId {
		StorageId::series(7)
	}

	fn part(v: &str) -> Partition {
		Partition::of(&[Value::Utf8(v.to_string())])
	}

	fn tags() -> Vec<Option<u8>> {
		vec![None, Some(0), Some(1), Some(7), Some(255)]
	}

	fn series_universe() -> Vec<EncodedKey> {
		let mut out = Vec::new();
		for tag in tags() {
			for key in [0u64, 1, 5, 100, u64::MAX] {
				for sequence in [0u64, 1, 3, u64::MAX] {
					out.push(SeriesRowKey {
						storage: storage(),
						variant_tag: tag,
						key,
						sequence,
					}
					.encode());
				}
			}
		}
		out
	}

	fn partitioned_universe() -> Vec<EncodedKey> {
		let mut out = Vec::new();
		for partition in [Partition(0), part("us"), part("eu"), Partition(u128::MAX)] {
			for tag in tags() {
				for key in [0u64, 5, u64::MAX] {
					for sequence in [0u64, 2, u64::MAX] {
						out.push(PartitionedSeriesRowKey {
							storage: storage(),
							partition,
							variant_tag: tag,
							key,
							sequence,
						}
						.encode());
					}
				}
			}
		}
		out
	}

	fn byte_bound(bound: &Bound<EncodedKey>) -> Bound<&[u8]> {
		match bound {
			Bound::Included(key) => Bound::Included(key.as_slice()),
			Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
			Bound::Unbounded => Bound::Unbounded,
		}
	}

	fn columns_of(schema: SqliteSchema, key: &EncodedKey) -> Vec<i64> {
		match schema {
			SqliteSchema::Series => {
				let c = series_ident_of(key.as_slice()).expect("a series key decodes").to_sql_columns();
				vec![c.variant_tag, c.key, c.sequence]
			}
			_ => {
				let c = partitioned_series_ident_of(key.as_slice())
					.expect("a partitioned series key decodes")
					.to_sql_columns();
				vec![c.partition_hi, c.partition_lo, c.variant_tag, c.key, c.sequence]
			}
		}
	}

	fn selected_by_columns(schema: SqliteSchema, universe: &[EncodedKey], range: &EncodedKeyRange) -> Vec<usize> {
		let widths = series_suffix_widths(schema).expect("a series schema names its field widths");
		let header = series_storage_header(schema, storage()).expect("a series schema names its header");
		let bounds = series_range_bounds(
			header.as_slice(),
			widths,
			byte_bound(&range.start),
			byte_bound(&range.end),
		)
		.expect("every bound this suite builds is a bound of the table it names");
		let (lower, upper) = match bounds {
			SeriesRangeBounds::Empty => return Vec::new(),
			SeriesRangeBounds::Range {
				lower,
				upper,
			} => (lower, upper),
		};
		universe.iter()
			.enumerate()
			.filter(|(_, key)| {
				let columns = columns_of(schema, key);
				let above = match &lower {
					Bound::Included(v) => columns >= *v,
					Bound::Excluded(v) => columns > *v,
					Bound::Unbounded => true,
				};
				let below = match &upper {
					Bound::Included(v) => columns <= *v,
					Bound::Excluded(v) => columns < *v,
					Bound::Unbounded => true,
				};
				above && below
			})
			.map(|(i, _)| i)
			.collect()
	}

	fn selected_by_bytes(universe: &[EncodedKey], range: &EncodedKeyRange) -> Vec<usize> {
		universe.iter().enumerate().filter(|(_, key)| range.contains(key)).map(|(i, _)| i).collect()
	}

	fn assert_agrees(schema: SqliteSchema, universe: &[EncodedKey], range: &EncodedKeyRange, label: &str) -> usize {
		let by_bytes = selected_by_bytes(universe, range);
		let by_columns = selected_by_columns(schema, universe, range);
		assert_eq!(
			by_bytes, by_columns,
			"{label}: the narrow column range must select exactly the rows the encoded byte range does"
		);
		by_bytes.len()
	}

	#[test]
	fn the_column_tuple_orders_a_series_key_exactly_as_its_encoded_bytes_do() {
		// Every bound this module translates is a byte prefix reinterpreted as a column tuple. If the two
		// orders ever disagree, every range against a narrow table silently returns the wrong window.
		let mut by_bytes = series_universe();
		by_bytes.sort_by_key(|key| key.as_slice().to_vec());
		let mut by_columns = series_universe();
		by_columns.sort_by_key(|key| columns_of(SqliteSchema::Series, key));
		assert_eq!(by_bytes, by_columns);

		let mut partitioned_by_bytes = partitioned_universe();
		partitioned_by_bytes.sort_by_key(|key| key.as_slice().to_vec());
		let mut partitioned_by_columns = partitioned_universe();
		partitioned_by_columns.sort_by_key(|key| columns_of(SqliteSchema::PartitionedSeries, key));
		assert_eq!(partitioned_by_bytes, partitioned_by_columns);
	}

	#[test]
	fn an_absent_variant_tag_sorts_after_every_present_one_in_both_orders() {
		// The None sentinel is the one column value that is not a complement of its field, so it is the one
		// place the column order could part company with the byte order without any other test noticing.
		let none = SeriesRowKey {
			storage: storage(),
			variant_tag: None,
			key: 5,
			sequence: 0,
		}
		.encode();
		let tagged = SeriesRowKey {
			storage: storage(),
			variant_tag: Some(0),
			key: 5,
			sequence: 0,
		}
		.encode();
		assert!(tagged.as_slice() < none.as_slice());
		assert!(
			columns_of(SqliteSchema::Series, &tagged) < columns_of(SqliteSchema::Series, &none),
			"an absent tag must sort last in the column order too"
		);
	}

	#[test]
	fn a_storage_only_start_bound_admits_every_row_of_its_table() {
		// Shape one: the start of a full scan carries the kind byte and the object id and nothing else, so
		// it must translate to a bound below every row rather than to the zero tuple read as a real key.
		let universe = series_universe();
		let range = SeriesRowKeyRange::full_scan(storage(), None).encode();
		let header = series_storage_header(SqliteSchema::Series, storage()).unwrap();
		assert_eq!(
			range.start,
			Bound::Included(header.clone()),
			"a full scan start must be the bare storage header, which is the shape this test pins"
		);
		let selected = assert_agrees(SqliteSchema::Series, &universe, &range, "series full scan");
		assert_eq!(selected, universe.len(), "a full scan must reach every row");
	}

	#[test]
	fn a_tag_only_start_bound_admits_exactly_that_tag_and_everything_after_it() {
		// Shape two: the start carries the tag flag and the tag but no key. Padding the missing key with
		// anything but the lowest byte would drop the first rows of the tag the caller asked for.
		let universe = series_universe();
		for tag in [0u8, 1, 7, 255] {
			let range = SeriesRowKeyRange::scan_range(storage(), Some(tag), None, None, None).encode();
			let selected = assert_agrees(
				SqliteSchema::Series,
				&universe,
				&range,
				&format!("series tag only start, tag {tag}"),
			);
			assert!(selected > 0, "tag {tag} names rows that exist, so the range must not be empty");
			for i in selected_by_columns(SqliteSchema::Series, &universe, &range) {
				let row_tag = series_ident_of(universe[i].as_slice()).unwrap().variant_tag();
				assert!(
					row_tag.is_none_or(|t| t <= tag),
					"tag {tag} must not admit a row tagged {row_tag:?}, which sorts before it"
				);
			}
			assert_eq!(
				selected == universe.len(),
				tag == 255,
				"only the lowest tag in the descending order opens the scan to every row"
			);
		}
	}

	#[test]
	fn a_tag_and_key_start_bound_stops_short_of_the_next_key() {
		// Shape three: the start carries the tag and the key but no sequence. A prefix that padded the
		// sequence high would skip the first sequence of the boundary key, and one that translated the key
		// ascending would select the complement window.
		let universe = series_universe();
		for key_end in [1u64, 5, 100, u64::MAX] {
			let range =
				SeriesRowKeyRange::scan_range(storage(), Some(7), None, Some(key_end), None).encode();
			let selected = assert_agrees(
				SqliteSchema::Series,
				&universe,
				&range,
				&format!("series tag and key start, key_end {key_end}"),
			);
			assert!(selected > 0, "key_end {key_end} leaves rows below it, so the range must not be empty");
		}
	}

	#[test]
	fn a_full_key_end_bound_keeps_every_sequence_of_its_key() {
		// The end of a bounded scan is a complete key whose sequence is zero, which is the last sequence in
		// the descending order. Translating it exclusively would drop the caller's own boundary row.
		let universe = series_universe();
		for (key_start, key_end) in [(Some(1u64), None), (Some(5), Some(100)), (Some(0), Some(u64::MAX))] {
			let range =
				SeriesRowKeyRange::scan_range(storage(), Some(7), key_start, key_end, None).encode();
			let selected = assert_agrees(
				SqliteSchema::Series,
				&universe,
				&range,
				&format!("series bounded scan {key_start:?}..{key_end:?}"),
			);
			assert!(selected > 0, "a bounded scan over populated keys must not be empty");
		}
	}

	#[test]
	fn an_untagged_bounded_scan_admits_only_untagged_rows() {
		// With no tag but a key bound the encoder still writes the absent tag flag, so the bound is not a
		// storage wide one. Treating it as storage wide would mix every tagged row into an untagged scan.
		let universe = series_universe();
		let range = SeriesRowKeyRange::scan_range(storage(), None, Some(1), Some(100), None).encode();
		let selected = assert_agrees(SqliteSchema::Series, &universe, &range, "series untagged bounded scan");
		assert!(selected > 0);
		for i in selected_by_columns(SqliteSchema::Series, &universe, &range) {
			assert_eq!(
				series_ident_of(universe[i].as_slice()).unwrap().variant_tag(),
				None,
				"an untagged bounded scan must not admit a tagged row"
			);
		}
	}

	#[test]
	fn a_cursor_start_bound_excludes_the_row_it_names() {
		// A resumed scan hands back an exclusive full key. Translating it inclusively would replay the last
		// row of the previous page on every page boundary.
		let universe = series_universe();
		let cursor = AnyKey::from(SeriesRowKey {
			storage: storage(),
			variant_tag: Some(7),
			key: 5,
			sequence: 1,
		});
		let range = SeriesRowKeyRange::scan_range(storage(), Some(7), None, None, Some(&cursor)).encode();
		let selected = assert_agrees(SqliteSchema::Series, &universe, &range, "series resumed scan");
		assert!(selected > 0);
		let position = universe
			.iter()
			.position(|key| *key == cursor.encode())
			.expect("the cursor row is in the universe");
		assert!(
			!selected_by_columns(SqliteSchema::Series, &universe, &range).contains(&position),
			"the cursor row itself must not be replayed"
		);
	}

	#[test]
	fn a_range_that_names_no_rows_translates_to_no_rows() {
		// The empty sentinel is a pair of excluded empty keys, which start below every row of every table.
		// Reading it as an open lower bound would turn an empty scan into a full one.
		let universe = series_universe();
		let range = SeriesRowKeyRange::scan_range(storage(), None, None, Some(0), None).encode();
		assert_eq!(selected_by_bytes(&universe, &range), Vec::<usize>::new());
		assert_eq!(selected_by_columns(SqliteSchema::Series, &universe, &range), Vec::<usize>::new());
	}

	#[test]
	fn a_bound_that_names_a_different_object_is_read_as_an_edge_not_as_a_key() {
		// The end of a full scan names the previous object id, which is not a key of this table at all. It
		// must translate to an open upper bound, never to a tuple parsed out of the neighbouring id.
		let widths = series_suffix_widths(SqliteSchema::Series).unwrap();
		let header = series_storage_header(SqliteSchema::Series, storage()).unwrap();
		let end = SeriesRowKeyRange::full_scan(storage(), None).encode().end;
		let bounds =
			series_range_bounds(header.as_slice(), widths, Bound::Unbounded, byte_bound(&end)).unwrap();
		match bounds {
			SeriesRangeBounds::Range {
				upper: Bound::Unbounded,
				..
			} => {}
			_ => panic!("the past the end edge of a storage must translate to an open upper bound"),
		}
	}

	fn partial_key(schema: SqliteSchema, prefix_len: usize, key: &EncodedKey) -> EncodedKey {
		let header = series_storage_header(schema, storage()).unwrap();
		EncodedKey::new(&key.as_slice()[..header.len() + prefix_len])
	}

	#[test]
	fn a_partial_upper_bound_excludes_every_row_that_extends_it() {
		// The byte path accepts any bound, not only the shapes the range builders emit, and an included
		// partial upper bound is the one shape whose translation is not obvious: a full key that starts with
		// the prefix sorts after it, so the prefix must translate to an exclusive tuple, not an inclusive
		// one. Reading it inclusively pulls a whole tag, or a whole key, into a scan that stopped short of it.
		let universe = series_universe();
		let boundary = SeriesRowKey {
			storage: storage(),
			variant_tag: Some(7),
			key: 5,
			sequence: 0,
		}
		.encode();
		for prefix_len in [0usize, 2, 10] {
			let partial = partial_key(SqliteSchema::Series, prefix_len, &boundary);
			for end in [Bound::Included(partial.clone()), Bound::Excluded(partial.clone())] {
				let range = EncodedKeyRange::new(Bound::Unbounded, end);
				assert_agrees(
					SqliteSchema::Series,
					&universe,
					&range,
					&format!("partial upper bound of {prefix_len} suffix bytes"),
				);
			}
		}
	}

	#[test]
	fn a_partial_lower_bound_admits_every_row_that_extends_it() {
		// The mirror of the upper bound rule: a full key that starts with the prefix sorts after it, so an
		// excluded partial lower bound still admits every row under that prefix. Translating it exclusively
		// would drop the first row of the tag or key the caller asked for.
		let universe = series_universe();
		let boundary = SeriesRowKey {
			storage: storage(),
			variant_tag: Some(7),
			key: 5,
			sequence: 0,
		}
		.encode();
		for prefix_len in [0usize, 2, 10] {
			let partial = partial_key(SqliteSchema::Series, prefix_len, &boundary);
			for start in [Bound::Included(partial.clone()), Bound::Excluded(partial.clone())] {
				let range = EncodedKeyRange::new(start, Bound::Unbounded);
				assert_agrees(
					SqliteSchema::Series,
					&universe,
					&range,
					&format!("partial lower bound of {prefix_len} suffix bytes"),
				);
			}
		}
	}

	#[test]
	fn a_full_key_bound_honours_its_own_inclusivity() {
		// A complete key can be excluded, and that is what a resumed scan relies on. Collapsing the two
		// cases would either replay one row per page or skip one.
		let universe = series_universe();
		let boundary = SeriesRowKey {
			storage: storage(),
			variant_tag: Some(7),
			key: 5,
			sequence: 1,
		}
		.encode();
		for range in [
			EncodedKeyRange::new(Bound::Included(boundary.clone()), Bound::Unbounded),
			EncodedKeyRange::new(Bound::Excluded(boundary.clone()), Bound::Unbounded),
			EncodedKeyRange::new(Bound::Unbounded, Bound::Included(boundary.clone())),
			EncodedKeyRange::new(Bound::Unbounded, Bound::Excluded(boundary.clone())),
		] {
			assert_agrees(SqliteSchema::Series, &universe, &range, "full key bound");
		}
		let included = EncodedKeyRange::new(Bound::Included(boundary.clone()), Bound::Unbounded);
		let excluded = EncodedKeyRange::new(Bound::Excluded(boundary.clone()), Bound::Unbounded);
		assert_eq!(
			selected_by_columns(SqliteSchema::Series, &universe, &included).len(),
			selected_by_columns(SqliteSchema::Series, &universe, &excluded).len() + 1,
			"excluding a full key must drop exactly the row it names"
		);
	}

	#[test]
	fn a_partitioned_partial_bound_follows_the_same_rule_at_every_field_boundary() {
		// The partitioned suffix has five fields, so a bound can stop after the partition, after the tag or
		// after the key. Each stop must behave like the unpartitioned ones.
		let universe = partitioned_universe();
		let boundary = PartitionedSeriesRowKey {
			storage: storage(),
			partition: part("us"),
			variant_tag: Some(7),
			key: 5,
			sequence: 0,
		}
		.encode();
		for prefix_len in [0usize, 8, 16, 18, 26] {
			let partial = partial_key(SqliteSchema::PartitionedSeries, prefix_len, &boundary);
			for bound in [Bound::Included(partial.clone()), Bound::Excluded(partial.clone())] {
				assert_agrees(
					SqliteSchema::PartitionedSeries,
					&universe,
					&EncodedKeyRange::new(bound.clone(), Bound::Unbounded),
					&format!("partitioned partial lower bound of {prefix_len} bytes"),
				);
				assert_agrees(
					SqliteSchema::PartitionedSeries,
					&universe,
					&EncodedKeyRange::new(Bound::Unbounded, bound),
					&format!("partitioned partial upper bound of {prefix_len} bytes"),
				);
			}
		}
	}

	#[test]
	fn a_start_bound_past_the_end_of_the_table_selects_nothing() {
		// A start bound naming a later object id sorts after every row of this table. Reading it as an open
		// lower bound would turn a scan that names nothing into a full table scan.
		let widths = series_suffix_widths(SqliteSchema::Series).unwrap();
		let header = series_storage_header(SqliteSchema::Series, storage()).unwrap();
		let past_the_end = SeriesRowKeyRange::full_scan(storage(), None).encode().end;
		let bounds =
			series_range_bounds(header.as_slice(), widths, byte_bound(&past_the_end), Bound::Unbounded)
				.unwrap();
		assert!(
			matches!(bounds, SeriesRangeBounds::Empty),
			"a start bound above every row must select nothing, not everything"
		);
	}

	#[test]
	fn a_partitioned_full_scan_reaches_every_partition() {
		let universe = partitioned_universe();
		let range = PartitionedSeriesRowKeyRange::full_scan(storage()).encode();
		let selected =
			assert_agrees(SqliteSchema::PartitionedSeries, &universe, &range, "partitioned full scan");
		assert_eq!(selected, universe.len());
	}

	#[test]
	fn a_partition_prefix_range_admits_exactly_one_partition() {
		// The prefix form ends on the successor of the partition bytes, which is a byte string that is not a
		// key. Padding it the wrong way would either lose the last rows of the partition or reach into the
		// next one.
		let universe = partitioned_universe();
		for partition in [Partition(0), part("us"), part("eu"), Partition(u128::MAX)] {
			let range = PartitionedSeriesRowKeyRange::partition_range(storage(), partition).encode();
			let selected = assert_agrees(
				SqliteSchema::PartitionedSeries,
				&universe,
				&range,
				&format!("partition range {partition:?}"),
			);
			assert!(selected > 0, "every partition in the universe holds rows");
			for i in selected_by_columns(SqliteSchema::PartitionedSeries, &universe, &range) {
				assert_eq!(
					partitioned_series_ident_of(universe[i].as_slice()).unwrap().partition(),
					partition,
					"a partition range must not admit a row of another partition"
				);
			}
		}
	}

	#[test]
	fn a_partitioned_scan_translates_every_partial_shape_it_emits() {
		// The partitioned encoder emits the same three partial starts as the unpartitioned one, each with
		// sixteen partition bytes in front, plus a resumed start and a full key end.
		let universe = partitioned_universe();
		let cursor = AnyKey::from(PartitionedSeriesRowKey {
			storage: storage(),
			partition: part("us"),
			variant_tag: Some(7),
			key: 5,
			sequence: 0,
		});
		let cases: Vec<(&str, EncodedKeyRange)> = vec![
			(
				"partition and tag",
				PartitionedSeriesRowKeyRange::scan_range(
					storage(),
					part("us"),
					Some(7),
					None,
					None,
					None,
				)
				.encode(),
			),
			(
				"partition, tag and key end",
				PartitionedSeriesRowKeyRange::scan_range(
					storage(),
					part("us"),
					Some(7),
					None,
					Some(5),
					None,
				)
				.encode(),
			),
			(
				"partition, tag and both key bounds",
				PartitionedSeriesRowKeyRange::scan_range(
					storage(),
					part("us"),
					Some(7),
					Some(0),
					Some(u64::MAX),
					None,
				)
				.encode(),
			),
			(
				"partition untagged with key bounds",
				PartitionedSeriesRowKeyRange::scan_range(
					storage(),
					part("us"),
					None,
					Some(0),
					Some(u64::MAX),
					None,
				)
				.encode(),
			),
			(
				"resumed partition scan",
				PartitionedSeriesRowKeyRange::partition_scan_range(
					storage(),
					part("us"),
					Some(&cursor),
				)
				.encode(),
			),
			(
				"resumed full scan",
				PartitionedSeriesRowKeyRange::full_scan_range(storage(), Some(&cursor)).encode(),
			),
		];
		for (label, range) in cases {
			assert_agrees(SqliteSchema::PartitionedSeries, &universe, &range, label);
		}
	}

	#[test]
	fn a_suffix_longer_than_a_key_is_refused_rather_than_truncated() {
		// A bound with trailing bytes past the sequence is not a key of this table. Silently truncating it
		// would translate a bound the caller never asked for.
		let widths = series_suffix_widths(SqliteSchema::Series).unwrap();
		let header = series_storage_header(SqliteSchema::Series, storage()).unwrap();
		let mut too_long = SeriesRowKey {
			storage: storage(),
			variant_tag: Some(1),
			key: 1,
			sequence: 1,
		}
		.encode()
		.to_vec();
		too_long.push(0x00);
		assert!(series_range_bounds(header.as_slice(), widths, Bound::Included(&too_long), Bound::Unbounded)
			.is_none());
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::store::{EntryKind, EntryLayout};
	use reifydb_value::value::{partition::Partition, row_number::RowNumber};

	use super::*;
	use crate::tier::persistent::sqlite::entry::{SqliteSchema, sqlite_schema};

	#[test]
	fn a_higher_row_stores_a_lower_integer() {
		// SQLite orders the narrow column ascending, so the stored integer must fall as the row rises,
		// otherwise every scan of a narrow table runs opposite to the encoded key space it mirrors.
		assert!(row_to_sql(1) > row_to_sql(2));
		assert!(row_to_sql(2) > row_to_sql(u64::MAX));
		assert!(row_to_sql(0) > row_to_sql(1));
	}

	#[test]
	fn the_stored_integer_round_trips_across_the_whole_range() {
		for row in [0u64, 1, 2, 42, i64::MAX as u64, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
			assert_eq!(row_from_sql(row_to_sql(row)), row, "row {row} did not survive the mapping");
		}
	}

	#[test]
	fn the_stored_order_matches_the_encoded_key_order() {
		// The narrow column and the encoded key must agree, or a table that switches schema silently
		// reverses every cursor built against it.
		let storage = StorageId::table(1);
		let mut rows: Vec<u64> = vec![7, 1, 900, 0, 42, u64::MAX];

		let mut by_key = rows.clone();
		by_key.sort_by_key(|r| RowKey::encoded(storage, RowNumber(*r)).as_slice().to_vec());

		rows.sort_by_key(|r| row_to_sql(*r));

		assert_eq!(rows, by_key, "the stored integer must sort exactly as the encoded key does");
	}

	#[test]
	fn both_partition_halves_invert_together() {
		// A partition is one 128 bit value split across two columns. If only one half inverts, a pair
		// that differs in the high half orders against a pair that differs in the low half.
		let low = Partition(1);
		let high = Partition((1u128 << 64) | 1);

		let low_halves = (partition_half_to_sql((low.0 >> 64) as u64), partition_half_to_sql(low.0 as u64));
		let high_halves = (partition_half_to_sql((high.0 >> 64) as u64), partition_half_to_sql(high.0 as u64));

		assert!(low_halves > high_halves, "the larger partition must store the lower pair, as rows do");
		assert_eq!(partition_half_from_sql(low_halves.1), 1);
		assert_eq!(partition_half_from_sql(high_halves.0), 1);
	}

	#[test]
	fn a_row_range_keeps_the_start_below_the_end() {
		// The encoded start is the smaller key, so it must stay the smaller stored integer. Swapping
		// the two here is how the narrowed schema first disagreed with the blob one.
		let storage = StorageId::table(1);
		let start = RowKey::encoded(storage, RowNumber(9));
		let end = RowKey::encoded(storage, RowNumber(2));
		assert!(start.as_slice() < end.as_slice(), "row 9 must encode below row 2");

		let bounds = row_range_bounds(Bound::Included(start.as_slice()), Bound::Included(end.as_slice()));
		match (bounds.lower, bounds.upper) {
			(Bound::Included(lower), Bound::Included(upper)) => {
				assert!(lower < upper, "the start must bound below and the end above");
				assert_eq!(row_from_sql(lower), 9);
				assert_eq!(row_from_sql(upper), 2);
			}
			other => panic!("expected two included bounds, got {other:?}"),
		}
	}

	#[test]
	fn the_static_row_mapping_follows_the_layout_not_the_storage_kind() {
		// An entry names one layout, so a view's rows are as narrow as a table's and the storage kind does
		// not enter into it. The series arms record the same legacy default as the mapping test in entry.rs:
		// the per table probe, not this mapping, decides what an existing series table is read as.
		assert_eq!(sqlite_schema(EntryKind::Source(StorageId::table(1), EntryLayout::Row)), SqliteSchema::Row);
		assert_eq!(sqlite_schema(EntryKind::Source(StorageId::view(1), EntryLayout::Row)), SqliteSchema::Row);
		assert_eq!(
			sqlite_schema(EntryKind::Source(StorageId::series(1), EntryLayout::Series)),
			SqliteSchema::Blob
		);
		assert_eq!(
			sqlite_schema(EntryKind::Source(StorageId::view(1), EntryLayout::Series)),
			SqliteSchema::Blob
		);
		assert_eq!(
			sqlite_schema(EntryKind::PartitionedSource(StorageId::table(1), EntryLayout::Row)),
			SqliteSchema::Partitioned
		);
		assert_eq!(
			sqlite_schema(EntryKind::PartitionedSource(StorageId::view(1), EntryLayout::Row)),
			SqliteSchema::Partitioned
		);
	}
}
