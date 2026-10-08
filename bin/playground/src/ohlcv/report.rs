// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	fmt::Write as _,
	fs,
	path::Path,
};

pub struct ColumnSize {
	pub name: String,
	pub bytes: u64,
	pub encoding: String,
	pub encodings: BTreeMap<String, usize>,
}

#[derive(Default)]
pub struct ColumnTally {
	columns: HashMap<String, (u64, BTreeMap<String, usize>)>,
}

impl ColumnTally {
	pub fn add(&mut self, name: &str, bytes: u64, encoding: String) {
		self.merge(name, bytes, &BTreeMap::from([(encoding, 1)]));
	}

	pub fn merge(&mut self, name: &str, bytes: u64, encodings: &BTreeMap<String, usize>) {
		let entry = self.columns.entry(name.to_string()).or_default();
		entry.0 += bytes;
		for (encoding, count) in encodings {
			*entry.1.entry(encoding.clone()).or_default() += count;
		}
	}

	pub fn finish(mut self, order: &[String]) -> Vec<ColumnSize> {
		order.iter()
			.filter_map(|name| {
				let (bytes, encodings) = self.columns.remove(name)?;
				let total: usize = encodings.values().sum();
				let mut ranked: Vec<_> = encodings.clone().into_iter().collect();
				ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
				let encoding = if ranked.len() == 1 {
					ranked[0].0.clone()
				} else {
					ranked.iter()
						.take(3)
						.map(|(id, n)| format!("{id} {:.0}%", *n as f64 * 100.0 / total as f64))
						.collect::<Vec<_>>()
						.join(", ")
				};
				Some(ColumnSize {
					name: name.clone(),
					bytes,
					encoding,
					encodings,
				})
			})
			.collect()
	}
}

pub struct Row {
	pub phase: &'static str,
	pub dataset: String,
	pub layout: String,
	pub columns: String,
	pub profile: String,
	pub blocks: usize,
	pub rows: usize,
	pub bytes: u64,
	pub system_bytes: Option<u64>,
	pub decode_mbps: Option<f64>,
	pub verified: String,
	pub per_column: Vec<ColumnSize>,
}

const RAW_BYTES_PER_ROW: f64 = 48.0;

impl Row {
	fn per_row(&self, bytes: u64) -> f64 {
		bytes as f64 / self.rows.max(1) as f64
	}

	fn ratio(&self) -> f64 {
		RAW_BYTES_PER_ROW / self.per_row(self.user_bytes())
	}

	fn user_bytes(&self) -> u64 {
		self.bytes - self.system_bytes.unwrap_or(0)
	}
}

fn optional(value: Option<f64>, precision: usize) -> String {
	value.map_or_else(|| "-".to_string(), |v| format!("{v:.precision$}"))
}

pub fn print(title: &str, rows: &[Row]) {
	println!("\n{title}");
	println!(
		"{:<8} {:<16} {:<19} {:<8} {:>6} {:>9} {:>12} {:>7} {:>9} {:>6} {:>11}  verified",
		"dataset",
		"layout",
		"columns",
		"profile",
		"blocks",
		"rows",
		"bytes",
		"B/row",
		"sys B/row",
		"ratio",
		"decode MB/s"
	);
	for row in rows {
		println!(
			"{:<8} {:<16} {:<19} {:<8} {:>6} {:>9} {:>12} {:>7.2} {:>9} {:>5.1}x {:>11}  {}",
			row.dataset,
			row.layout,
			row.columns,
			row.profile,
			row.blocks,
			row.rows,
			row.bytes,
			row.per_row(row.user_bytes()),
			optional(row.system_bytes.map(|b| row.per_row(b)), 2),
			row.ratio(),
			optional(row.decode_mbps, 0),
			row.verified
		);
	}
	for row in rows {
		println!("per column ({}, {}, {}, {}):", row.dataset, row.layout, row.columns, row.profile);
		for column in &row.per_column {
			println!("  {:<14} {:>8.2} B/row  {}", column.name, row.per_row(column.bytes), column.encoding);
		}
	}
}

pub fn write_csv(path: &Path, rows: &[Row]) -> Result<(), String> {
	let mut out = String::from(
		"phase,dataset,layout,columns,profile,blocks,rows,bytes,user_b_per_row,system_b_per_row,ratio,decode_mb_s,verified\n",
	);
	for row in rows {
		writeln!(
			out,
			"{},{},\"{}\",\"{}\",{},{},{},{},{:.4},{},{:.3},{},\"{}\"",
			row.phase,
			row.dataset,
			row.layout,
			row.columns,
			row.profile,
			row.blocks,
			row.rows,
			row.bytes,
			row.per_row(row.user_bytes()),
			optional(row.system_bytes.map(|b| row.per_row(b)), 4),
			row.ratio(),
			optional(row.decode_mbps, 1),
			row.verified.replace('"', "'")
		)
		.map_err(|e| e.to_string())?;
	}
	if let Some(dir) = path.parent() {
		fs::create_dir_all(dir).map_err(|e| e.to_string())?;
	}
	fs::write(path, out).map_err(|e| e.to_string())
}
