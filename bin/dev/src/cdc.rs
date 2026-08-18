// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::cdc;
use reifydb_core::{
	event::metric::CdcEviction,
	interface::{
		catalog::object::ObjectId,
		cdc::{Cdc, SystemChange},
		change::{Change, ChangeOrigin, Diff},
	},
	key::{EncodableKey, Key, kind::KeyKind, row::RowKey, series_row::SeriesRowKey},
};
use rusqlite::{Connection, OpenFlags};

use crate::Result;

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
	pub change_bytes: u64,
	pub system_changes: u64,
	pub system_bytes: u64,
	pub empty_commits: u64,
	pub origins: BTreeMap<Origin, Slice>,
	pub diff_kinds: BTreeMap<&'static str, Slice>,
	pub system_kinds: BTreeMap<String, Slice>,
	pub storage_rows: BTreeMap<u64, Slice>,
	pub undecodable_row_keys: u64,
	pub decode_failures: u64,
}

pub fn scan(dir: &str, include_blocks: bool) -> Result<Stats> {
	let path = std::path::Path::new(dir).join("cdc.db");
	if !path.exists() {
		return Err(format!("no cdc.db in '{dir}'"));
	}
	let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
		.map_err(|e| format!("failed to open '{}' read-only: {e}", path.display()))?;

	let mut stats = Stats::default();
	stats.min_version = u64::MAX;

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
			Ok(entry) => absorb(&entry, stats),
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
					absorb(entry, stats);
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

fn absorb(cdc: &Cdc, stats: &mut Stats) {
	stats.min_version = stats.min_version.min(cdc.version.0);
	stats.max_version = stats.max_version.max(cdc.version.0);
	stats.payload_raw += encoded_len(cdc);

	if cdc.changes.is_empty() && cdc.system_changes.is_empty() {
		stats.empty_commits += 1;
	}

	for change in &cdc.changes {
		absorb_change(change, stats);
	}
	for change in &cdc.system_changes {
		absorb_system_change(change, stats);
	}
}

fn absorb_change(change: &Change, stats: &mut Stats) {
	let bytes = encoded_len(change);
	let rows = change.row_count() as u64;

	stats.changes += 1;
	stats.change_bytes += bytes;
	stats.origins.entry(origin_of(&change.origin)).or_default().add(rows, bytes);

	for diff in change.diffs.iter() {
		stats.diff_kinds.entry(diff_kind(diff)).or_default().add(diff.row_count() as u64, encoded_len(diff));
	}
}

fn absorb_system_change(change: &SystemChange, stats: &mut Stats) {
	let bytes = encoded_len(change);
	stats.system_changes += 1;
	stats.system_bytes += bytes;
	stats.system_kinds.entry(system_kind(change)).or_default().add(1, bytes);

	match Key::kind(change.key().as_slice()) {
		Some(KeyKind::Row) => match RowKey::decode(change.key()) {
			Some(key) => stats.storage_rows.entry(key.storage.as_u64()).or_default().add(1, bytes),
			None => stats.undecodable_row_keys += 1,
		},
		Some(KeyKind::SeriesRow) => match SeriesRowKey::decode(change.key()) {
			Some(key) => stats.storage_rows.entry(key.series.0).or_default().add(1, bytes),
			None => stats.undecodable_row_keys += 1,
		},
		_ => {}
	}
}

fn encoded_len<T: serde::Serialize>(value: &T) -> u64 {
	to_stdvec(value).map(|v| v.len() as u64).unwrap_or(0)
}

fn origin_of(origin: &ChangeOrigin) -> Origin {
	match origin {
		ChangeOrigin::Object(id) => Origin {
			kind: object_kind(id),
			id: id.to_u64(),
		},
		ChangeOrigin::Flow(id) => Origin {
			kind: "operator",
			id: id.0,
		},
	}
}

fn object_kind(id: &ObjectId) -> &'static str {
	match id {
		ObjectId::Table(_) => "table",
		ObjectId::View(_) => "view",
		ObjectId::TableVirtual(_) => "vtable",
		ObjectId::RingBuffer(_) => "ringbuffer",
		ObjectId::Dictionary(_) => "dictionary",
		ObjectId::Series(_) => "series",
		ObjectId::Queue(_) => "queue",
	}
}

fn diff_kind(diff: &Diff) -> &'static str {
	match diff {
		Diff::Insert {
			..
		} => "insert",
		Diff::Update {
			..
		} => "update",
		Diff::Remove {
			..
		} => "remove",
	}
}

fn system_kind(change: &SystemChange) -> String {
	let op = match change {
		SystemChange::Insert {
			..
		} => "insert",
		SystemChange::Update {
			..
		} => "update",
		SystemChange::Delete {
			..
		} => "delete",
	};
	match Key::kind(change.key().as_slice()) {
		Some(kind) => format!("{kind:?}/{op}"),
		None => format!("(undecodable)/{op}"),
	}
}
