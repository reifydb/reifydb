// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{catalog::Catalog, cdc};

fn pct(part: u64, whole: u64) -> String {
	if whole == 0 {
		"0.0%".to_string()
	} else {
		format!("{:.1}%", part as f64 * 100.0 / whole as f64)
	}
}

fn fmt_bytes(b: u64) -> String {
	const UNITS: &[(&str, f64)] = &[("GB", 1e9), ("MB", 1e6), ("KB", 1e3)];
	for (suffix, div) in UNITS {
		if b as f64 >= *div {
			return format!("{:.1} {}", b as f64 / div, suffix);
		}
	}
	format!("{b} B")
}

fn group_int(n: u64) -> String {
	let s = n.to_string();
	let bytes = s.as_bytes();
	let mut out = String::new();
	for (i, c) in bytes.iter().enumerate() {
		if i > 0 && (bytes.len() - i).is_multiple_of(3) {
			out.push(',');
		}
		out.push(*c as char);
	}
	out
}

fn table(headers: &[&str], rows: &[Vec<String>]) {
	let n = headers.len();
	let mut width = headers.iter().map(|h| h.len()).collect::<Vec<_>>();
	for r in rows {
		for (i, c) in r.iter().enumerate() {
			width[i] = width[i].max(c.len());
		}
	}
	let render = |cells: &[String]| {
		let mut line = String::new();
		for (i, c) in cells.iter().enumerate() {
			if i + 1 == n {
				line.push_str(c);
			} else {
				line.push_str(&format!("{:>w$}  ", c, w = width[i]));
			}
		}
		println!("{}", line.trim_end());
	};
	render(&headers.iter().map(|h| h.to_string()).collect::<Vec<_>>());
	for r in rows {
		render(r);
	}
}

pub struct CdcOptions {
	pub all: bool,
	pub top: usize,
	pub json: bool,
}

pub fn render_cdc(cat: Option<&Catalog>, s: &cdc::Stats, file_bytes: u64, opts: CdcOptions) {
	if opts.json {
		render_cdc_json(cat, s, file_bytes);
		return;
	}

	let commits = s.live_rows + s.block_entries;
	let stored = s.payload_stored + s.rollup_stored;

	println!("# cdc.db {} on disk", fmt_bytes(file_bytes));
	println!(
		"  {} commits ({} live rows, {} entries in {} blocks), versions {}..{}",
		group_int(commits),
		group_int(s.live_rows),
		group_int(s.block_entries),
		group_int(s.blocks),
		group_int(s.min_version),
		group_int(s.max_version)
	);
	println!(
		"  {} cdc changes, {} empty commits, {} rollup entries",
		group_int(s.cdc_changes),
		group_int(s.empty_commits),
		group_int(s.rollup_entries)
	);
	println!(
		"  rebuild: {} changes over {} row keys, {} object slots touched",
		group_int(s.changes),
		group_int(s.attributed_rows),
		group_int(s.touched_objects)
	);
	if s.decode_failures > 0 {
		println!("  WARNING: {} rows failed to decode", group_int(s.decode_failures));
	}

	println!("\n## blob (stored bytes are what the row costs on disk)");
	table(
		&["STORED", "RAW", "RATIO", "%", "BLOB"],
		&[
			vec![
				fmt_bytes(s.payload_stored),
				fmt_bytes(s.payload_raw),
				ratio(s.payload_raw, s.payload_stored),
				pct(s.payload_stored, stored),
				"payload  zstd-1(postcard)".to_string(),
			],
			vec![
				fmt_bytes(s.rollup_stored),
				fmt_bytes(s.rollup_raw),
				ratio(s.rollup_raw, s.rollup_stored),
				pct(s.rollup_stored, stored),
				"stats_rollup  postcard, uncompressed".to_string(),
			],
		],
	);
	println!("stored total: {} ({} of the file)", fmt_bytes(stored), pct(stored, file_bytes));

	println!("\n## row keys by operation");
	let mut kinds: Vec<(&&str, &cdc::Slice)> = s.row_kinds.iter().collect();
	kinds.sort_by(|a, b| b.1.bytes.cmp(&a.1.bytes));
	table(
		&["RAW", "%", "ROWS", "OPERATION"],
		&kinds.iter()
			.map(|(k, v)| {
				vec![fmt_bytes(v.bytes), pct(v.bytes, s.row_bytes), group_int(v.rows), k.to_string()]
			})
			.collect::<Vec<_>>(),
	);
	println!(
		"{} row keys = {} attributed to an object; {} cdc changes carry no row key",
		group_int(s.row_changes),
		group_int(s.attributed_rows),
		group_int(s.cdc_changes - s.row_changes)
	);
	if s.undecodable_row_keys > 0 {
		println!("WARNING: {} row keys failed to decode", group_int(s.undecodable_row_keys));
	}

	println!("\n## by object");
	let mut objects: Vec<(&cdc::Origin, &cdc::ObjectRows)> = s.objects.iter().collect();
	objects.sort_by(|a, b| b.1.rows.bytes.cmp(&a.1.rows.bytes));
	let shown = if opts.all {
		objects.len()
	} else {
		opts.top.min(objects.len())
	};
	table(
		&["RAW", "%", "CHANGES", "ROWS", "OBJECT", "LOGICAL"],
		&objects[..shown]
			.iter()
			.map(|(o, v)| {
				vec![
					fmt_bytes(v.rows.bytes),
					pct(v.rows.bytes, s.row_bytes),
					group_int(v.changes),
					group_int(v.rows.rows),
					format!("{}_{}", o.kind, o.id),
					object_label(cat, o),
				]
			})
			.collect::<Vec<_>>(),
	);
	if shown < objects.len() {
		println!("\n{} of {} objects shown (use --all)", shown, objects.len());
	}

	println!("\n## cdc changes by key kind");
	let mut sys: Vec<(&String, &cdc::Slice)> = s.cdc_kinds.iter().collect();
	sys.sort_by(|a, b| b.1.bytes.cmp(&a.1.bytes));
	let sys_shown = if opts.all {
		sys.len()
	} else {
		opts.top.min(sys.len())
	};
	table(
		&["RAW", "%", "COUNT", "KEY KIND / OP"],
		&sys[..sys_shown]
			.iter()
			.map(|(k, v)| {
				vec![fmt_bytes(v.bytes), pct(v.bytes, s.cdc_bytes), group_int(v.count), k.to_string()]
			})
			.collect::<Vec<_>>(),
	);
	if sys_shown < sys.len() {
		println!("\n{} of {} key kinds shown (use --all)", sys_shown, sys.len());
	}
}

fn render_cdc_json(cat: Option<&Catalog>, s: &cdc::Stats, file_bytes: u64) {
	print_json(&[
		("record", json_str("summary")),
		("file_bytes", file_bytes.to_string()),
		("live_rows", s.live_rows.to_string()),
		("blocks", s.blocks.to_string()),
		("block_entries", s.block_entries.to_string()),
		("min_version", s.min_version.to_string()),
		("max_version", s.max_version.to_string()),
		("payload_stored", s.payload_stored.to_string()),
		("payload_raw", s.payload_raw.to_string()),
		("rollup_stored", s.rollup_stored.to_string()),
		("rollup_entries", s.rollup_entries.to_string()),
		("changes", s.changes.to_string()),
		("touched_objects", s.touched_objects.to_string()),
		("cdc_changes", s.cdc_changes.to_string()),
		("cdc_bytes", s.cdc_bytes.to_string()),
		("row_changes", s.row_changes.to_string()),
		("row_bytes", s.row_bytes.to_string()),
		("attributed_rows", s.attributed_rows.to_string()),
		("empty_commits", s.empty_commits.to_string()),
		("undecodable_row_keys", s.undecodable_row_keys.to_string()),
		("decode_failures", s.decode_failures.to_string()),
	]);
	for (kind, v) in &s.row_kinds {
		print_json(&[
			("record", json_str("row_kind")),
			("operation", json_str(kind)),
			("rows", v.rows.to_string()),
			("raw_bytes", v.bytes.to_string()),
		]);
	}
	for (o, v) in &s.objects {
		print_json(&[
			("record", json_str("object")),
			("object", json_str(&format!("{}_{}", o.kind, o.id))),
			("logical", json_str(&object_label(cat, o))),
			("changes", v.changes.to_string()),
			("rows", v.rows.rows.to_string()),
			("raw_bytes", v.rows.bytes.to_string()),
		]);
	}
	for (kind, v) in &s.cdc_kinds {
		print_json(&[
			("record", json_str("cdc_kind")),
			("kind", json_str(kind)),
			("count", v.count.to_string()),
			("raw_bytes", v.bytes.to_string()),
		]);
	}
}

fn object_label(cat: Option<&Catalog>, o: &cdc::Origin) -> String {
	let Some(cat) = cat else {
		return String::new();
	};
	if o.kind == "view" {
		if let Some(name) = cat.views.get(&o.id) {
			return format!("{name}  [view]");
		}
	}
	cat.sources.get(&o.id).map(|(name, k)| format!("{name}  [{k}]")).unwrap_or_else(|| "(unmapped)".to_string())
}

fn ratio(raw: u64, stored: u64) -> String {
	if stored == 0 {
		"-".to_string()
	} else {
		format!("{:.2}x", raw as f64 / stored as f64)
	}
}

pub fn dump_catalog(cat: &Catalog, json: bool) {
	let mut sources: Vec<(&u64, &(String, &str))> = cat.sources.iter().collect();
	sources.sort_by_key(|(id, _)| **id);
	let mut operators: Vec<(&u64, &(String, String))> = cat.operators.iter().collect();
	operators.sort_by_key(|(id, _)| **id);

	if json {
		for (id, (name, kind)) in &sources {
			print_json(&[
				("source_id", id.to_string()),
				("name", json_str(name)),
				("kind", json_str(kind)),
			]);
		}
		for (id, (view, stage)) in &operators {
			print_json(&[
				("operator_id", id.to_string()),
				("view", json_str(view)),
				("stage", json_str(stage)),
			]);
		}
		return;
	}

	println!("# {} sources, {} flow-node operators\n", sources.len(), operators.len());
	println!("## source_<id> -> name");
	for (id, (name, kind)) in &sources {
		println!("  source_{:<6} {}  [{}]", id, name, kind);
	}
	println!("\n## operator_<id> -> view [stage]{{operator}}");
	for (id, (view, stage)) in &operators {
		println!("  operator_{:<5} {}  {}", id, view, stage);
	}
}

fn json_str(s: &str) -> String {
	let mut out = String::from("\"");
	for c in s.chars() {
		match c {
			'"' => out.push_str("\\\""),
			'\\' => out.push_str("\\\\"),
			'\n' => out.push_str("\\n"),
			'\t' => out.push_str("\\t"),
			_ => out.push(c),
		}
	}
	out.push('"');
	out
}

fn print_json(fields: &[(&str, String)]) {
	let body = fields.iter().map(|(k, v)| format!("\"{k}\":{v}")).collect::<Vec<_>>().join(",");
	println!("{{{body}}}");
}
