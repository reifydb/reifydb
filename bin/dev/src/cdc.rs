// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	path::Path,
};

use postcard::{from_bytes, to_stdvec};
use reifydb_cdc::rebuild::{changed_objects, row_target};
use reifydb_codec::cdc;
use reifydb_core::{
	event::metric::CdcEviction,
	interface::{
		catalog::object::ObjectId,
		cdc::{Cdc, CdcChange},
	},
	key::kind::KeyKind,
};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use crate::Result;

pub const INSERT: &str = "insert";
pub const UPDATE: &str = "update";
pub const REMOVE: &str = "remove";

#[derive(Default, Clone)]
pub struct Slice {
	pub count: u64,
	pub rows: u64,
	pub bytes: u64,
}

impl Slice {
	fn add(&mut self, rows: u64, bytes: u64) {
		self.count += 1;
		self.rows += rows;
		self.bytes += bytes;
	}
}

#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Origin {
	pub kind: &'static str,
	pub id: u64,
}

#[derive(Default, Clone)]
pub struct ObjectRows {
	pub changes: u64,
	pub rows: Slice,
}

#[derive(Default)]
pub struct Stats {
	pub live_rows: u64,
	pub blocks: u64,
	pub block_entries: u64,
	pub payload_stored: u64,
	pub payload_raw: u64,
	pub rollup_stored: u64,
	pub rollup_raw: u64,
	pub rollup_entries: u64,
	pub min_version: u64,
	pub max_version: u64,
	pub changes: u64,
	pub touched_objects: u64,
	pub cdc_changes: u64,
	pub cdc_bytes: u64,
	pub row_changes: u64,
	pub row_bytes: u64,
	pub attributed_rows: u64,
	pub empty_commits: u64,
	pub objects: BTreeMap<Origin, ObjectRows>,
	pub row_kinds: BTreeMap<&'static str, Slice>,
	pub cdc_kinds: BTreeMap<String, Slice>,
	pub undecodable_row_keys: u64,
	pub decode_failures: u64,
}

pub fn scan(dir: &str, include_blocks: bool) -> Result<Stats> {
	let path = Path::new(dir).join("cdc.db");
	if !path.exists() {
		return Err(format!("no cdc.db in '{dir}'"));
	}
	let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
		.map_err(|e| format!("failed to open '{}' read-only: {e}", path.display()))?;

	let mut stats = Stats {
		min_version: u64::MAX,
		..Default::default()
	};

	scan_live(&conn, &mut stats)?;
	if include_blocks {
		scan_blocks(&conn, &mut stats)?;
	}
	if stats.min_version == u64::MAX {
		stats.min_version = 0;
	}
	Ok(stats)
}

fn scan_live(conn: &Connection, stats: &mut Stats) -> Result<()> {
	let mut stmt = conn
		.prepare(r#"SELECT payload, stats_rollup FROM "cdc" ORDER BY version ASC"#)
		.map_err(|e| format!("prepare live scan: {e}"))?;
	let mut rows = stmt.query([]).map_err(|e| format!("query live rows: {e}"))?;

	while let Some(row) = rows.next().map_err(|e| format!("read live row: {e}"))? {
		let payload: Vec<u8> = row.get(0).map_err(|e| format!("read payload: {e}"))?;
		let rollup: Vec<u8> = row.get(1).map_err(|e| format!("read stats_rollup: {e}"))?;

		stats.live_rows += 1;
		stats.payload_stored += payload.len() as u64;
		stats.rollup_stored += rollup.len() as u64;
		absorb_rollup(&rollup, stats);

		match cdc::decode::<Cdc>(&payload) {
			Ok(entry) => absorb(&entry, stats)?,
			Err(_) => stats.decode_failures += 1,
		}
	}
	Ok(())
}

fn scan_blocks(conn: &Connection, stats: &mut Stats) -> Result<()> {
	let mut stmt = match conn
		.prepare(r#"SELECT payload, stats_rollup, num_entries FROM "cdc_block" ORDER BY max_version ASC"#)
	{
		Ok(stmt) => stmt,
		Err(_) => return Ok(()),
	};
	let mut rows = stmt.query([]).map_err(|e| format!("query block rows: {e}"))?;

	while let Some(row) = rows.next().map_err(|e| format!("read block row: {e}"))? {
		let payload: Vec<u8> = row.get(0).map_err(|e| format!("read block payload: {e}"))?;
		let rollup: Vec<u8> = row.get(1).map_err(|e| format!("read block stats_rollup: {e}"))?;
		let entries: i64 = row.get(2).map_err(|e| format!("read num_entries: {e}"))?;

		stats.blocks += 1;
		stats.block_entries += entries.max(0) as u64;
		stats.payload_stored += payload.len() as u64;
		stats.rollup_stored += rollup.len() as u64;
		absorb_rollup(&rollup, stats);

		match cdc::decode::<Vec<Cdc>>(&payload) {
			Ok(items) => {
				for entry in &items {
					absorb(entry, stats)?;
				}
			}
			Err(_) => stats.decode_failures += 1,
		}
	}
	Ok(())
}

fn absorb_rollup(bytes: &[u8], stats: &mut Stats) {
	stats.rollup_raw += bytes.len() as u64;
	if let Ok(entries) = from_bytes::<Vec<CdcEviction>>(bytes) {
		stats.rollup_entries += entries.len() as u64;
	}
}

fn absorb(cdc: &Cdc, stats: &mut Stats) -> Result<()> {
	stats.min_version = stats.min_version.min(cdc.version.0);
	stats.max_version = stats.max_version.max(cdc.version.0);
	stats.payload_raw += encoded_len(cdc)?;

	if cdc.changes.is_empty() {
		stats.empty_commits += 1;
	}
	stats.touched_objects += changed_objects(cdc).len() as u64;

	let mut rebuilt: BTreeSet<Origin> = BTreeSet::new();
	for change in &cdc.changes {
		absorb_cdc_change(change, stats, &mut rebuilt)?;
	}

	stats.changes += rebuilt.len() as u64;
	for origin in rebuilt {
		stats.objects.entry(origin).or_default().changes += 1;
	}
	Ok(())
}

fn absorb_cdc_change(change: &CdcChange, stats: &mut Stats, rebuilt: &mut BTreeSet<Origin>) -> Result<()> {
	let bytes = encoded_len(change)?;
	stats.cdc_changes += 1;
	stats.cdc_bytes += bytes;
	stats.cdc_kinds.entry(cdc_change_kind(change)).or_default().add(1, bytes);

	if !matches!(
		KeyKind::of(change.key().as_slice()),
		Some(KeyKind::Row | KeyKind::SeriesRow | KeyKind::PartitionedRow)
	) {
		return Ok(());
	}
	let Some(kind) = row_kind(change) else {
		return Ok(());
	};
	stats.row_changes += 1;
	stats.row_bytes += bytes;

	let Some(target) = row_target(change.key()) else {
		stats.undecodable_row_keys += 1;
		return Ok(());
	};

	let origin = origin_of(target.object);
	stats.objects.entry(origin.clone()).or_default().rows.add(1, bytes);
	stats.attributed_rows += 1;
	stats.row_kinds.entry(kind).or_default().add(1, bytes);
	rebuilt.insert(origin);
	Ok(())
}

fn row_kind(change: &CdcChange) -> Option<&'static str> {
	match change {
		CdcChange::Insert {
			..
		} => Some(INSERT),
		CdcChange::Update {
			..
		} => Some(UPDATE),
		CdcChange::Delete {
			visible: true,
			..
		} => Some(REMOVE),
		CdcChange::Delete {
			visible: false,
			..
		} => None,
	}
}

fn origin_of(object: ObjectId) -> Origin {
	let kind = match object {
		ObjectId::Table(_) => "table",
		ObjectId::View(_) => "view",
		ObjectId::TableVirtual(_) => "vtable",
		ObjectId::RingBuffer(_) => "ringbuffer",
		ObjectId::Dictionary(_) => "dictionary",
		ObjectId::Series(_) => "series",
		ObjectId::Queue(_) => "queue",
	};
	Origin {
		kind,
		id: object.as_u64(),
	}
}

fn encoded_len<T: Serialize>(value: &T) -> Result<u64> {
	to_stdvec(value).map(|v| v.len() as u64).map_err(|e| format!("encode for byte accounting: {e}"))
}

fn cdc_change_kind(change: &CdcChange) -> String {
	let op = match change {
		CdcChange::Insert {
			..
		} => "insert",
		CdcChange::Update {
			..
		} => "update",
		CdcChange::Delete {
			visible: true,
			..
		} => "delete",
		CdcChange::Delete {
			visible: false,
			..
		} => "delete(hidden)",
	};
	match KeyKind::of(change.key().as_slice()) {
		Some(kind) => format!("{kind:?}/{op}"),
		None => format!("(undecodable)/{op}"),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{id::NamespaceId, storage::StorageId},
		key::{namespace::NamespaceKey, row::RowKey},
	};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use super::*;

	fn commit(changes: Vec<CdcChange>) -> Cdc {
		Cdc::new(CommitVersion(1), DateTime::from_nanos(0), changes)
	}

	fn row() -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![0u8; 16]))
	}

	fn insert(storage: StorageId, id: u64) -> CdcChange {
		CdcChange::Insert {
			key: RowKey::encoded(storage, RowNumber(id)),
			post: row(),
		}
	}

	fn delete(storage: StorageId, id: u64, visible: bool) -> CdcChange {
		CdcChange::Delete {
			key: RowKey::encoded(storage, RowNumber(id)),
			pre: Some(row()),
			visible,
		}
	}

	fn namespace_insert() -> CdcChange {
		CdcChange::Insert {
			key: NamespaceKey::encoded(NamespaceId(1)),
			post: row(),
		}
	}

	fn stats_of(records: &[Cdc]) -> Stats {
		// A failed encode here would silently zero the byte columns, so the fixture must panic, not absorb it.
		let mut stats = Stats::default();
		for record in records {
			absorb(record, &mut stats).expect("fixture records must encode");
		}
		stats
	}

	fn object<'a>(stats: &'a Stats, kind: &'static str, id: u64) -> &'a ObjectRows {
		stats.objects
			.get(&Origin {
				kind,
				id,
			})
			.unwrap_or_else(|| panic!("no {kind}_{id} in the report"))
	}

	#[test]
	fn an_invisible_delete_is_absent_from_the_report_exactly_as_it_is_from_the_rebuild() {
		// The report must not invent a row the rebuild skipped, or the two disagree on what happened.
		let stats = stats_of(&[commit(vec![delete(StorageId::table(7), 1, false)])]);

		assert_eq!(stats.row_changes, 0, "an invisible delete must not be counted as a row key");
		assert_eq!(stats.attributed_rows, 0);
		assert_eq!(stats.changes, 0, "the rebuild must emit no change for an invisible delete");
		assert!(stats.objects.is_empty(), "and it must open no object slot of its own");
	}

	#[test]
	fn only_the_visible_of_two_byte_identical_deletes_reaches_the_report() {
		// Both keys decode to the same object, so only the visibility flag can tell them apart.
		let stats = stats_of(&[commit(vec![
			delete(StorageId::view(4), 1, true),
			delete(StorageId::view(4), 2, false),
		])]);

		assert_eq!(stats.row_changes, 1, "byte-identical keys must be split by the visibility flag alone");
		assert_eq!(stats.attributed_rows, 1);
		assert_eq!(stats.changes, 1, "the visible delete alone must rebuild one change for the view");
		assert_eq!(object(&stats, "view", 4).rows.rows, 1);
	}

	#[test]
	fn a_view_row_is_attributed_to_the_view_and_never_to_the_table_sharing_its_id() {
		let stats = stats_of(&[commit(vec![insert(StorageId::view(42), 1), insert(StorageId::table(42), 1)])]);

		assert_eq!(object(&stats, "view", 42).rows.rows, 1, "a view must never report under a table id");
		assert_eq!(object(&stats, "table", 42).rows.rows, 1);
		assert_eq!(stats.changes, 2, "two objects touched in one commit must rebuild two changes");
	}

	#[test]
	fn every_counted_row_key_is_attributed_to_an_object() {
		// A row key counted but attributed nowhere is a row no line of the report accounts for.
		let stats = stats_of(&[commit(vec![
			insert(StorageId::table(7), 1),
			delete(StorageId::table(7), 2, false),
			insert(StorageId::series(9), 1),
			namespace_insert(),
		])]);

		assert_eq!(stats.cdc_changes, 4);
		assert_eq!(stats.row_changes, 2, "a namespace key and an invisible delete are both not row keys");
		assert_eq!(stats.attributed_rows, stats.row_changes);
		assert_eq!(stats.undecodable_row_keys, 0);
		assert_eq!(
			stats.cdc_bytes,
			stats.row_bytes
				+ encoded_len(&namespace_insert()).unwrap()
				+ encoded_len(&delete(StorageId::table(7), 2, false)).unwrap(),
			"bytes skipped by the rebuild still have to show up in the system total"
		);
	}

	#[test]
	fn a_commit_counts_one_rebuilt_change_per_object_however_many_rows_it_touched() {
		let stats = stats_of(&[commit(vec![
			insert(StorageId::table(7), 1),
			insert(StorageId::table(7), 2),
			insert(StorageId::table(7), 3),
		])]);

		assert_eq!(stats.changes, 1, "the stored stream is consolidated per object per commit");
		assert_eq!(object(&stats, "table", 7).changes, 1);
		assert_eq!(object(&stats, "table", 7).rows.rows, 3);
	}
}
