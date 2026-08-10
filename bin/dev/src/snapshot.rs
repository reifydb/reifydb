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
	pub max_bytes: u64,
	pub groups: HashSet<(u64, u64)>,
}

impl Census {
	pub fn bytes(&self) -> u64 {
		self.key_bytes + self.value_bytes
	}

	pub fn avg_bytes(&self) -> f64 {
		if self.rows == 0 {
			0.0
		} else {
			self.bytes() as f64 / self.rows as f64
		}
	}

	pub fn avg_bytes_per_group(&self) -> f64 {
		if self.groups.is_empty() {
			0.0
		} else {
			self.bytes() as f64 / self.groups.len() as f64
		}
	}

	pub fn operators(&self) -> usize {
		self.groups.iter().map(|(operator, _)| *operator).collect::<HashSet<u64>>().len()
	}

	fn record(&mut self, operator: u64, group: Option<u64>, key_len: u64, value_len: u64) {
		self.rows += 1;
		self.key_bytes += key_len;
		self.value_bytes += value_len;
		self.max_bytes = self.max_bytes.max(key_len + value_len);
		if let Some(group) = group {
			self.groups.insert((operator, group));
		}
	}
}

pub struct Options {
	pub operator: Option<u64>,
	pub top: usize,
	pub json: bool,
	pub global: bool,
}

pub fn report(operator_db: &str, cat: Option<&Catalog>, opts: Options) -> Result<(), String> {
	let conn = Connection::open_with_flags(operator_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
		.map_err(|e| format!("failed to open '{operator_db}' read-only: {e}"))?;

	let mut by_operator: HashMap<(u64, u64, String), Census> = HashMap::new();
	let mut by_keyspace: HashMap<String, Census> = HashMap::new();

	for (operator, generation, chunk_count, manifest_bytes) in manifests(&conn, opts.operator)? {
		by_operator.entry((operator, generation, MANIFEST.to_string())).or_default().record(
			operator,
			None,
			0,
			manifest_bytes,
		);
		by_keyspace.entry(MANIFEST.to_string()).or_default().record(operator, None, 0, manifest_bytes);

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
				let (label, group) = classify(key);
				by_operator
					.entry((operator, generation, label.clone()))
					.or_default()
					.record(operator, group, key.len() as u64, value_len as u64);
				by_keyspace.entry(label).or_default().record(
					operator,
					group,
					key.len() as u64,
					value_len as u64,
				);
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

	if opts.global {
		render_global(by_keyspace, &opts);
	} else {
		render(by_operator, cat, &opts);
	}
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
	let grand_rows: u64 = tally.values().map(|c| c.rows).sum();
	let grand_bytes: u64 = tally.values().map(|c| c.bytes()).sum();
	match cat {
		Some(cat) => render_by_view(tally, cat, grand_rows, grand_bytes, opts),
		None => render_flat(tally, grand_rows, grand_bytes, opts),
	}
}

fn share_pct(part: u64, total: u64) -> f64 {
	if total == 0 {
		0.0
	} else {
		part as f64 * 100.0 / total as f64
	}
}

fn render_flat(tally: HashMap<(u64, u64, String), Census>, grand_rows: u64, grand_bytes: u64, opts: &Options) {
	let mut rows: Vec<((u64, u64, String), Census)> = tally.into_iter().collect();
	rows.sort_by(|a, b| b.1.bytes().cmp(&a.1.bytes()).then_with(|| a.0.cmp(&b.0)));

	if opts.json {
		for ((operator, generation, label), census) in rows.iter().take(shown(rows.len(), opts.top)) {
			println!(
				"{{\"operator\":{operator},\"generation\":{generation},\"keyspace\":\"{label}\",\
				 \"rows\":{},\"bytes\":{},\"key_bytes\":{},\"value_bytes\":{},\"avg_bytes\":{:.1},\
				 \"max_bytes\":{},\"groups\":{},\"avg_bytes_per_group\":{:.1}}}",
				census.rows,
				census.bytes(),
				census.key_bytes,
				census.value_bytes,
				census.avg_bytes(),
				census.max_bytes,
				census.groups.len(),
				census.avg_bytes_per_group()
			);
		}
		return;
	}

	println!(
		"{:>12} {:>4}  {:<22} {:>9} {:>10} {:>10} {:>10} {:>6} {:>9} {:>7} {:>10}",
		"PHYSICAL", "GEN", "KEYSPACE", "ROWS", "KEY_BYTES", "VALUE_BYTES", "BYTES", "%", "AVG/ROW", "GROUPS", "MAX"
	);
	let limit = shown(rows.len(), opts.top);
	for ((operator, generation, label), census) in rows.iter().take(limit) {
		println!(
			"{:>12} {:>4}  {:<22} {:>9} {:>10} {:>10} {:>10} {:>5.1}% {:>9.0} {:>7} {:>10}",
			format!("operator_{operator}"),
			generation,
			label,
			census.rows,
			human(census.key_bytes),
			human(census.value_bytes),
			human(census.bytes()),
			share_pct(census.bytes(), grand_bytes),
			census.avg_bytes(),
			census.groups.len(),
			human(census.max_bytes)
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
		 (logical payload bytes: key+value only, does not include SQLite page/index overhead; \
		 pass --names to group by view)",
		grand_rows,
		human(grand_bytes),
		operators.len(),
		generations.len(),
		rows.len()
	);
}

fn render_by_view(
	tally: HashMap<(u64, u64, String), Census>,
	cat: &Catalog,
	grand_rows: u64,
	grand_bytes: u64,
	opts: &Options,
) {
	let mut rows: Vec<((String, String, u64, u64, String), Census)> = tally
		.into_iter()
		.map(|((operator, generation, keyspace), census)| {
			let (view, stage) = cat
				.operators
				.get(&operator)
				.cloned()
				.unwrap_or_else(|| (format!("(unmapped operator_{operator})"), String::new()));
			((view, stage, operator, generation, keyspace), census)
		})
		.collect();

	let mut view_bytes: HashMap<String, u64> = HashMap::new();
	for ((view, ..), census) in &rows {
		*view_bytes.entry(view.clone()).or_default() += census.bytes();
	}

	rows.sort_by(|a, b| {
		let view_a = view_bytes.get(&a.0.0).copied().unwrap_or(0);
		let view_b = view_bytes.get(&b.0.0).copied().unwrap_or(0);
		view_b
			.cmp(&view_a)
			.then_with(|| a.0.0.cmp(&b.0.0))
			.then_with(|| b.1.bytes().cmp(&a.1.bytes()))
			.then_with(|| a.0.cmp(&b.0))
	});

	if opts.json {
		for ((view, stage, operator, generation, keyspace), census) in
			rows.iter().take(shown(rows.len(), opts.top))
		{
			println!(
				"{{\"view\":\"{}\",\"stage\":\"{}\",\"operator\":{operator},\"generation\":{generation},\
				 \"keyspace\":\"{keyspace}\",\"rows\":{},\"bytes\":{},\"key_bytes\":{},\"value_bytes\":{},\
				 \"avg_bytes\":{:.1},\"max_bytes\":{},\"groups\":{},\"avg_bytes_per_group\":{:.1}}}",
				view.replace('"', "'"),
				stage.replace('"', "'"),
				census.rows,
				census.bytes(),
				census.key_bytes,
				census.value_bytes,
				census.avg_bytes(),
				census.max_bytes,
				census.groups.len(),
				census.avg_bytes_per_group()
			);
		}
		return;
	}

	println!(
		"{:<32} {:<20} {:>8} {:>4}  {:<22} {:>9} {:>10} {:>10} {:>10} {:>6} {:>9} {:>7} {:>10}",
		"VIEW",
		"STAGE",
		"OPERATOR",
		"GEN",
		"KEYSPACE",
		"ROWS",
		"KEY_BYTES",
		"VALUE_BYTES",
		"BYTES",
		"%",
		"AVG/ROW",
		"GROUPS",
		"MAX"
	);
	let limit = shown(rows.len(), opts.top);
	for ((view, stage, operator, generation, keyspace), census) in rows.iter().take(limit) {
		println!(
			"{:<32} {:<20} {:>8} {:>4}  {:<22} {:>9} {:>10} {:>10} {:>10} {:>5.1}% {:>9.0} {:>7} {:>10}",
			view,
			stage,
			operator,
			generation,
			keyspace,
			census.rows,
			human(census.key_bytes),
			human(census.value_bytes),
			human(census.bytes()),
			share_pct(census.bytes(), grand_bytes),
			census.avg_bytes(),
			census.groups.len(),
			human(census.max_bytes)
		);
	}
	if rows.len() > limit {
		println!("\n{} of {} rows shown ({} hidden; use --top)", limit, rows.len(), rows.len() - limit);
	}
	println!(
		"\ngrand total: {} rows, {} across {} views, {} keyspaces (sorted by each view's total bytes; \
		 logical payload bytes: key+value only, does not include SQLite page/index overhead)",
		grand_rows,
		human(grand_bytes),
		view_bytes.len(),
		rows.len()
	);
}

fn render_global(tally: HashMap<String, Census>, opts: &Options) {
	let mut rows: Vec<(String, Census)> = tally.into_iter().collect();
	rows.sort_by(|a, b| b.1.bytes().cmp(&a.1.bytes()).then_with(|| a.0.cmp(&b.0)));
	let grand_rows: u64 = rows.iter().map(|(_, c)| c.rows).sum();
	let grand_bytes: u64 = rows.iter().map(|(_, c)| c.bytes()).sum();

	if opts.json {
		for (label, census) in rows.iter().take(shown(rows.len(), opts.top)) {
			println!(
				"{{\"keyspace\":\"{label}\",\"rows\":{},\"bytes\":{},\"key_bytes\":{},\
				 \"value_bytes\":{},\"avg_bytes\":{:.1},\"max_bytes\":{},\"groups\":{},\
				 \"avg_bytes_per_group\":{:.1},\"operators\":{}}}",
				census.rows,
				census.bytes(),
				census.key_bytes,
				census.value_bytes,
				census.avg_bytes(),
				census.max_bytes,
				census.groups.len(),
				census.avg_bytes_per_group(),
				census.operators()
			);
		}
		return;
	}

	println!(
		"{:<22} {:>10} {:>10} {:>10} {:>10} {:>6} {:>9} {:>9} {:>7} {:>10}",
		"KEYSPACE", "ROWS", "KEY_BYTES", "VALUE_BYTES", "BYTES", "%", "AVG/ROW", "OPERATORS", "GROUPS", "MAX"
	);
	let limit = shown(rows.len(), opts.top);
	for (label, census) in rows.iter().take(limit) {
		let share = if grand_bytes == 0 {
			0.0
		} else {
			census.bytes() as f64 * 100.0 / grand_bytes as f64
		};
		println!(
			"{:<22} {:>10} {:>10} {:>10} {:>10} {:>5.1}% {:>9.0} {:>9} {:>7} {:>10}",
			label,
			census.rows,
			human(census.key_bytes),
			human(census.value_bytes),
			human(census.bytes()),
			share,
			census.avg_bytes(),
			census.operators(),
			census.groups.len(),
			human(census.max_bytes)
		);
	}
	if rows.len() > limit {
		println!("\n{} of {} keyspaces shown ({} hidden; use --top)", limit, rows.len(), rows.len() - limit);
	}
	println!(
		"\ngrand total: {} rows, {} across {} keyspaces (global rollup across every operator and generation; \
		 GROUPS/OPERATORS undercount rows whose entry carried no group, e.g. the <manifest> row)",
		grand_rows,
		human(grand_bytes),
		rows.len()
	);
}
