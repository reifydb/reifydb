// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	codec::key::encoded::EncodedKey,
	core::key::{
		EncodableKey,
		operator_group_state::{OperatorGroupStateKey, is_framed_inner},
		operator_state::OperatorStateKey,
	},
};
use rusqlite::{Connection, OpenFlags};

use crate::catalog::Catalog;

const UNFRAMED: &str = "<unframed>";
const UNDECODABLE: &str = "<undecodable>";

#[derive(Default, Clone)]
pub struct Census {
	pub rows: u64,
	pub tombstones: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
}

impl Census {
	pub fn bytes(&self) -> u64 {
		self.key_bytes + self.value_bytes
	}

	pub fn live(&self) -> u64 {
		self.rows.saturating_sub(self.tombstones)
	}

	pub fn dead_share(&self) -> f64 {
		match self.rows {
			0 => 0.0,
			total => self.tombstones as f64 * 100.0 / total as f64,
		}
	}
}

pub struct Options {
	pub operator: Option<u64>,
	pub top: usize,
	pub json: bool,
	pub groups: bool,
}

pub fn keyspace(multi_db: &str, cat: Option<&Catalog>, opts: Options) -> Result<(), String> {
	let conn = Connection::open_with_flags(multi_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
		.map_err(|e| format!("failed to open '{multi_db}' read-only: {e}"))?;

	let mut tally: HashMap<(u64, String), Census> = HashMap::new();
	let mut groups: HashMap<(u64, String), u64> = HashMap::new();
	let mut seen_groups: HashMap<(u64, String), Vec<u64>> = HashMap::new();

	for operator in operator_tables(&conn, opts.operator)? {
		let table = format!("operator_{operator}__current");
		let mut stmt = conn
			.prepare(&format!(
				"SELECT key, length(coalesce(value, x'')), value IS NULL FROM \"{table}\""
			))
			.map_err(|e| format!("failed to scan {table}: {e}"))?;
		let mut q = stmt.query([]).map_err(|e| format!("failed to scan {table}: {e}"))?;
		while let Some(row) = q.next().map_err(|e| format!("failed to read {table}: {e}"))? {
			let key: Vec<u8> = row.get(0).map_err(|e| format!("failed to read {table}: {e}"))?;
			let value_len: i64 = row.get(1).map_err(|e| format!("failed to read {table}: {e}"))?;
			let dead: bool = row.get(2).map_err(|e| format!("failed to read {table}: {e}"))?;
			let (label, group) = classify(&key);
			let entry = tally.entry((operator, label.clone())).or_default();
			entry.rows += 1;
			entry.tombstones += u64::from(dead);
			entry.key_bytes += key.len() as u64;
			entry.value_bytes += value_len.max(0) as u64;
			if opts.groups
				&& let Some(group) = group
			{
				let seen = seen_groups.entry((operator, label.clone())).or_default();
				if !seen.contains(&group) {
					seen.push(group);
					*groups.entry((operator, label)).or_default() += 1;
				}
			}
		}
	}

	render(tally, groups, cat, &opts);
	Ok(())
}

fn classify(key: &[u8]) -> (String, Option<u64>) {
	let Some(decoded) = OperatorStateKey::decode(&EncodedKey::new(key.to_vec())) else {
		return (UNDECODABLE.to_string(), None);
	};
	if !is_framed_inner(&decoded.key) {
		return (UNFRAMED.to_string(), None);
	}
	match OperatorGroupStateKey::decode_inner(&decoded.key) {
		Some((group, keyspace, _)) => (keyspace.name().to_string(), Some(group.0)),
		None => (UNDECODABLE.to_string(), None),
	}
}

fn operator_tables(conn: &Connection, only: Option<u64>) -> Result<Vec<u64>, String> {
	let mut stmt = conn
		.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'operator_%__current'")
		.map_err(|e| format!("failed to list operator tables: {e}"))?;
	let mut q = stmt.query([]).map_err(|e| format!("failed to list operator tables: {e}"))?;
	let mut out = Vec::new();
	while let Some(row) = q.next().map_err(|e| format!("failed to list operator tables: {e}"))? {
		let name: String = row.get(0).map_err(|e| format!("failed to list operator tables: {e}"))?;
		let Some(rest) = name.strip_prefix("operator_") else {
			continue;
		};
		let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
		let Ok(id) = digits.parse::<u64>() else {
			continue;
		};
		if only.is_none_or(|want| want == id) {
			out.push(id);
		}
	}
	out.sort_unstable();
	Ok(out)
}

fn render(
	tally: HashMap<(u64, String), Census>,
	groups: HashMap<(u64, String), u64>,
	cat: Option<&Catalog>,
	opts: &Options,
) {
	let mut rows: Vec<((u64, String), Census)> = tally.into_iter().collect();
	rows.sort_by(|a, b| b.1.rows.cmp(&a.1.rows).then_with(|| a.0.cmp(&b.0)));
	let grand_rows: u64 = rows.iter().map(|(_, c)| c.rows).sum();
	let grand_bytes: u64 = rows.iter().map(|(_, c)| c.bytes()).sum();

	if opts.json {
		for ((operator, label), census) in rows.iter().take(shown(rows.len(), opts.top)) {
			let logical = cat.and_then(|c| c.operators.get(operator)).cloned().unwrap_or_default();
			println!(
				"{{\"operator\":{operator},\"keyspace\":\"{label}\",\"rows\":{},\"live\":{},\
				 \"tombstones\":{},\"bytes\":{},\"key_bytes\":{},\"value_bytes\":{},\"groups\":{},\
				 \"logical\":\"{}\"}}",
				census.rows,
				census.live(),
				census.tombstones,
				census.bytes(),
				census.key_bytes,
				census.value_bytes,
				groups.get(&(*operator, label.clone())).copied().unwrap_or(0),
				logical.replace('"', "'")
			);
		}
		return;
	}

	println!(
		"{:>12}  {:<22} {:>12} {:>12} {:>10} {:>7} {:>10} {:>6}{}  {}",
		"PHYSICAL",
		"KEYSPACE",
		"ROWS",
		"LIVE",
		"TOMB",
		"DEAD%",
		"BYTES",
		"%",
		if opts.groups {
			format!(" {:>9}", "GROUPS")
		} else {
			String::new()
		},
		"LOGICAL"
	);
	let limit = shown(rows.len(), opts.top);
	for ((operator, label), census) in rows.iter().take(limit) {
		let share = if grand_rows == 0 {
			0.0
		} else {
			census.rows as f64 * 100.0 / grand_rows as f64
		};
		let logical = cat.and_then(|c| c.operators.get(operator)).cloned().unwrap_or_default();
		let group_col = if opts.groups {
			format!(" {:>9}", groups.get(&(*operator, label.clone())).copied().unwrap_or(0))
		} else {
			String::new()
		};
		println!(
			"{:>12}  {:<22} {:>12} {:>12} {:>10} {:>6.1}% {:>10} {:>5.1}%{}  {}",
			format!("operator_{operator}"),
			label,
			census.rows,
			census.live(),
			census.tombstones,
			census.dead_share(),
			human(census.bytes()),
			share,
			group_col,
			logical
		);
	}
	if rows.len() > limit {
		println!("\n{} of {} rows shown ({} hidden; use --top)", limit, rows.len(), rows.len() - limit);
	}
	let grand_tombstones: u64 = rows.iter().map(|(_, c)| c.tombstones).sum();
	println!(
		"\ngrand total: {} rows ({} live, {} tombstones = {:.1}% dead), {} across {} keyspaces",
		grand_rows,
		grand_rows - grand_tombstones,
		grand_tombstones,
		if grand_rows == 0 {
			0.0
		} else {
			grand_tombstones as f64 * 100.0 / grand_rows as f64
		},
		human(grand_bytes),
		rows.len()
	);
}

fn shown(len: usize, top: usize) -> usize {
	if top == 0 {
		len
	} else {
		top.min(len)
	}
}

fn human(bytes: u64) -> String {
	const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
	let mut value = bytes as f64;
	let mut unit = 0;
	while value >= 1024.0 && unit + 1 < UNITS.len() {
		value /= 1024.0;
		unit += 1;
	}
	if unit == 0 {
		format!("{bytes} B")
	} else {
		format!("{value:.1} {}", UNITS[unit])
	}
}
