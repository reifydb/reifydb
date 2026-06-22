// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use rusqlite::{Connection, Error, OpenFlags};

#[derive(Default, Clone)]
pub struct Phys {
	pub current: u64,
	pub version: u64,
	pub historical: u64,
	pub rows: u64,
	pub rows_exact: bool,
}

impl Phys {
	pub fn total(&self) -> u64 {
		self.current + self.version + self.historical
	}
}

pub type Map = HashMap<(&'static str, u64), Phys>;

enum Tier {
	Current,
	Version,
	Historical,
}

pub fn read(multi_db: &str, exact_rows: bool) -> Result<Map, String> {
	let conn = Connection::open_with_flags(multi_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
		.map_err(|e| format!("failed to open '{multi_db}' read-only: {e}"))?;

	let mut map: Map = HashMap::new();
	{
		let mut stmt = conn
			.prepare(
				"SELECT name, SUM(pgsize), SUM(CASE WHEN pagetype='leaf' THEN ncell ELSE 0 END) \
				 FROM dbstat GROUP BY name",
			)
			.map_err(dberr)?;
		let mut q = stmt.query([]).map_err(dberr)?;
		while let Some(row) = q.next().map_err(dberr)? {
			let name: String = row.get(0).map_err(dberr)?;
			let bytes: i64 = row.get(1).map_err(dberr)?;
			let leaf_cells: i64 = row.get(2).map_err(dberr)?;
			let Some((kind, id, tier)) = parse_phys(&name) else {
				continue;
			};
			let entry = map.entry((kind, id)).or_default();
			let bytes = bytes.max(0) as u64;
			match tier {
				Tier::Current => {
					entry.current += bytes;
					entry.rows = leaf_cells.max(0) as u64;
				}
				Tier::Version => entry.version += bytes,
				Tier::Historical => entry.historical += bytes,
			}
		}
	}

	if exact_rows {
		let keys: Vec<_> = map.keys().copied().collect();
		for (kind, id) in keys {
			let table = format!("{kind}_{id}__current");
			if let Ok(count) =
				conn.query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |r| r.get::<_, i64>(0))
			{
				let entry = map.get_mut(&(kind, id)).unwrap();
				entry.rows = count.max(0) as u64;
				entry.rows_exact = true;
			}
		}
	}

	Ok(map)
}

fn parse_phys(name: &str) -> Option<(&'static str, u64, Tier)> {
	let (kind, rest) = if let Some(r) = name.strip_prefix("source_") {
		("source", r)
	} else if let Some(r) = name.strip_prefix("operator_") {
		("operator", r)
	} else {
		return None;
	};
	let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
	let id: u64 = digits.parse().ok()?;
	let tier = if name.ends_with("__version") {
		Tier::Version
	} else if name.contains("__historical") {
		Tier::Historical
	} else {
		Tier::Current
	};
	Some((kind, id, tier))
}

fn dberr(e: Error) -> String {
	format!("dbstat read failed: {e} (the bundled SQLite must be built with SQLITE_ENABLE_DBSTAT_VTAB)")
}
