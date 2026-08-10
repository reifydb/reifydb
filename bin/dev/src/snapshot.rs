// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, OpenFlags, Result as SqliteResult, params};

use crate::{
	catalog::Catalog,
	operator::{classify, human, shown},
};

const MANIFEST: &str = "<manifest>";

#[derive(Default, Clone)]
pub struct Census {
	pub rows: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
}

impl Census {
	pub fn bytes(&self) -> u64 {
		self.key_bytes + self.value_bytes
	}
}

pub struct Options {
	pub operator: Option<u64>,
	pub top: usize,
	pub json: bool,
}

pub fn report(operator_db: &str, cat: Option<&Catalog>, opts: Options) -> Result<(), String> {
	let conn = Connection::open_with_flags(operator_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
		.map_err(|e| format!("failed to open '{operator_db}' read-only: {e}"))?;

	let mut tally: HashMap<(u64, u64, String), Census> = HashMap::new();

	for (operator, generation, chunk_count, manifest_bytes) in manifests(&conn, opts.operator)? {
		let manifest_entry = tally.entry((operator, generation, MANIFEST.to_string())).or_default();
		manifest_entry.rows += 1;
		manifest_entry.value_bytes += manifest_bytes;

		let mut stmt = conn
			.prepare(
				"SELECT bytes FROM \"snapshot_chunk\" WHERE operator = ?1 AND generation = ?2 ORDER BY seq ASC",
			)
			.map_err(|e| {
				format!(
					"failed to prepare chunk scan for operator {operator} generation {generation}: {e}"
				)
			})?;
		let mut rows = stmt.query(params![operator as i64, generation as i64]).map_err(|e| {
			format!("failed to scan chunks for operator {operator} generation {generation}: {e}")
		})?;
		let mut seen: u64 = 0;
		while let Some(row) = rows.next().map_err(|e| {
			format!("failed to read chunk for operator {operator} generation {generation}: {e}")
		})? {
			let bytes: Vec<u8> = row.get(0).map_err(|e| {
				format!(
					"failed to read chunk bytes for operator {operator} generation {generation}: {e}"
				)
			})?;
			decode_chunk(&bytes, |key, value_len| {
				let (label, _group) = classify(key);
				let entry = tally.entry((operator, generation, label)).or_default();
				entry.rows += 1;
				entry.key_bytes += key.len() as u64;
				entry.value_bytes += value_len as u64;
			})
			.ok_or_else(|| {
				format!("malformed snapshot chunk for operator {operator} generation {generation}")
			})?;
			seen += 1;
		}
		if seen != chunk_count {
			return Err(format!(
				"operator {operator} generation {generation}: manifest expects {chunk_count} chunks, found {seen}"
			));
		}
	}

	render(tally, cat, &opts);
	Ok(())
}

fn manifests(conn: &Connection, only: Option<u64>) -> Result<Vec<(u64, u64, u64, u64)>, String> {
	let mut stmt = conn
		.prepare(
			r#"SELECT operator, generation, chunk_count, length(content_hash) + length(dictionary_max)
			   FROM "snapshot_manifest" ORDER BY operator ASC, generation DESC"#,
		)
		.map_err(|e| format!("failed to list snapshot manifests: {e}"))?;
	let rows = stmt
		.query_map([], |row| {
			Ok((
				row.get::<_, i64>(0)? as u64,
				row.get::<_, i64>(1)? as u64,
				row.get::<_, i64>(2)? as u64,
				row.get::<_, i64>(3)? as u64,
			))
		})
		.and_then(|rows| rows.collect::<SqliteResult<Vec<_>>>())
		.map_err(|e| format!("failed to read snapshot manifests: {e}"))?;
	Ok(rows.into_iter().filter(|(operator, ..)| only.is_none_or(|want| want == *operator)).collect())
}

fn decode_chunk(bytes: &[u8], mut on_entry: impl FnMut(&[u8], usize)) -> Option<()> {
	let mut offset = 0usize;
	while offset < bytes.len() {
		let key = decode_field(bytes, &mut offset)?;
		let value = decode_field(bytes, &mut offset)?;
		on_entry(key, value.len());
	}
	Some(())
}

fn decode_field<'a>(bytes: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
	let len_end = offset.checked_add(4)?;
	let len = u32::from_le_bytes(bytes.get(*offset..len_end)?.try_into().ok()?) as usize;
	let field_end = len_end.checked_add(len)?;
	let field = bytes.get(len_end..field_end)?;
	*offset = field_end;
	Some(field)
}

fn render(tally: HashMap<(u64, u64, String), Census>, cat: Option<&Catalog>, opts: &Options) {
	let mut rows: Vec<((u64, u64, String), Census)> = tally.into_iter().collect();
	rows.sort_by(|a, b| b.1.bytes().cmp(&a.1.bytes()).then_with(|| a.0.cmp(&b.0)));
	let grand_rows: u64 = rows.iter().map(|(_, c)| c.rows).sum();
	let grand_bytes: u64 = rows.iter().map(|(_, c)| c.bytes()).sum();

	if opts.json {
		for ((operator, generation, label), census) in rows.iter().take(shown(rows.len(), opts.top)) {
			let logical = cat.and_then(|c| c.operators.get(operator)).cloned().unwrap_or_default();
			println!(
				"{{\"operator\":{operator},\"generation\":{generation},\"keyspace\":\"{label}\",\
				 \"rows\":{},\"bytes\":{},\"key_bytes\":{},\"value_bytes\":{},\"logical\":\"{}\"}}",
				census.rows,
				census.bytes(),
				census.key_bytes,
				census.value_bytes,
				logical.replace('"', "'")
			);
		}
		return;
	}

	println!(
		"{:>12} {:>4}  {:<22} {:>12} {:>10} {:>6}  LOGICAL",
		"PHYSICAL", "GEN", "KEYSPACE", "ROWS", "BYTES", "%"
	);
	let limit = shown(rows.len(), opts.top);
	for ((operator, generation, label), census) in rows.iter().take(limit) {
		let share = if grand_bytes == 0 {
			0.0
		} else {
			census.bytes() as f64 * 100.0 / grand_bytes as f64
		};
		let logical = cat.and_then(|c| c.operators.get(operator)).cloned().unwrap_or_default();
		println!(
			"{:>12} {:>4}  {:<22} {:>12} {:>10} {:>5.1}%  {}",
			format!("operator_{operator}"),
			generation,
			label,
			census.rows,
			human(census.bytes()),
			share,
			logical
		);
	}
	if rows.len() > limit {
		println!("\n{} of {} rows shown ({} hidden; use --top)", limit, rows.len(), rows.len() - limit);
	}
	let operators: HashSet<u64> = rows.iter().map(|((operator, ..), _)| *operator).collect();
	let generations: HashSet<(u64, u64)> =
		rows.iter().map(|((operator, generation, _), _)| (*operator, *generation)).collect();
	println!(
		"\ngrand total: {} rows, {} across {} operators, {} operator-generations, {} keyspaces \
		 (logical payload bytes: key+value only, does not include SQLite page/index overhead)",
		grand_rows,
		human(grand_bytes),
		operators.len(),
		generations.len(),
		rows.len()
	);
}
