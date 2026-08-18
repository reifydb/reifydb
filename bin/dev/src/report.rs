// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use crate::{catalog::Catalog, cdc, dbstat};

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum GroupBy {
	Namespace,
	Tier,
}

pub struct Options {
	pub group_by: Option<GroupBy>,
	pub all: bool,
	pub top: usize,
	pub filter: Option<String>,
	pub json: bool,
	pub show_rows: bool,
}

struct Obj {
	phys: String,
	logical: String,
	p: dbstat::Phys,
}

pub fn render(cat: &Catalog, phys: &dbstat::Map, opts: Options) {
	let mut objs: Vec<Obj> = phys
		.iter()
		.map(|((kind, id), p)| {
			let logical = match *kind {
				"source" => cat
					.sources
					.get(id)
					.map(|(name, k)| format!("{name}  [{k}]"))
					.unwrap_or_else(|| "(unmapped)".to_string()),
				_ => cat.operators
					.get(id)
					.map(|(view, stage)| format!("{view}  {stage}"))
					.unwrap_or_else(|| "(unmapped)".to_string()),
			};
			Obj {
				phys: format!("{kind}_{id}"),
				logical,
				p: p.clone(),
			}
		})
		.collect();

	if let Some(f) = &opts.filter {
		objs.retain(|o| o.logical.contains(f.as_str()));
	}

	let grand: u64 = objs.iter().map(|o| o.p.total()).sum();

	match opts.group_by {
		Some(GroupBy::Tier) => render_tiers(&objs, grand, &opts),
		Some(GroupBy::Namespace) => render_groups(&objs, grand, &opts),
		None => render_objects(&mut objs, grand, &opts),
	}
}

fn render_objects(objs: &mut [Obj], grand: u64, opts: &Options) {
	objs.sort_by(|a, b| b.p.total().cmp(&a.p.total()));
	let shown = if opts.all {
		objs.len()
	} else {
		opts.top.min(objs.len())
	};

	if opts.json {
		for o in &objs[..shown] {
			print_json(&[
				("physical", json_str(&o.phys)),
				("logical", json_str(&o.logical)),
				("total_bytes", o.p.total().to_string()),
				("current_bytes", o.p.current.to_string()),
				("version_bytes", o.p.version.to_string()),
				("historical_bytes", o.p.historical.to_string()),
				("rows", o.p.rows.to_string()),
				("rows_exact", o.p.rows_exact.to_string()),
			]);
		}
		return;
	}

	let mut headers = vec!["PHYSICAL", "TOTAL", "CURRENT", "VER"];
	if opts.show_rows {
		headers.push("ROWS");
	}
	headers.push("%");
	headers.push("LOGICAL");

	let rows: Vec<Vec<String>> = objs[..shown]
		.iter()
		.map(|o| {
			let mut r = vec![
				o.phys.clone(),
				fmt_bytes(o.p.total()),
				fmt_bytes(o.p.current),
				fmt_bytes(o.p.version),
			];
			if opts.show_rows {
				r.push(fmt_rows(&o.p));
			}
			r.push(pct(o.p.total(), grand));
			r.push(o.logical.clone());
			r
		})
		.collect();

	table(&headers, &rows);
	footer(grand, objs.len(), shown);
}

fn render_groups(objs: &[Obj], grand: u64, opts: &Options) {
	let mut agg: BTreeMap<String, (dbstat::Phys, usize)> = BTreeMap::new();
	for o in objs {
		let e = agg.entry(parent_ns(&o.logical)).or_default();
		e.0.current += o.p.current;
		e.0.version += o.p.version;
		e.0.historical += o.p.historical;
		e.0.rows += o.p.rows;
		e.1 += 1;
	}
	let mut groups: Vec<(String, dbstat::Phys, usize)> = agg.into_iter().map(|(k, (p, n))| (k, p, n)).collect();
	groups.sort_by(|a, b| b.1.total().cmp(&a.1.total()));
	let shown = if opts.all {
		groups.len()
	} else {
		opts.top.min(groups.len())
	};

	if opts.json {
		for (k, p, n) in &groups[..shown] {
			print_json(&[
				("group", json_str(k)),
				("objects", n.to_string()),
				("total_bytes", p.total().to_string()),
				("current_bytes", p.current.to_string()),
				("version_bytes", p.version.to_string()),
			]);
		}
		return;
	}

	let mut headers = vec!["TOTAL", "CURRENT", "VER"];
	if opts.show_rows {
		headers.push("ROWS");
	}
	headers.push("%");
	headers.push("OBJ");
	headers.push("GROUP");

	let rows: Vec<Vec<String>> = groups[..shown]
		.iter()
		.map(|(k, p, n)| {
			let mut r = vec![fmt_bytes(p.total()), fmt_bytes(p.current), fmt_bytes(p.version)];
			if opts.show_rows {
				r.push(group_int(p.rows));
			}
			r.push(pct(p.total(), grand));
			r.push(n.to_string());
			r.push(k.clone());
			r
		})
		.collect();

	table(&headers, &rows);
	footer(grand, groups.len(), shown);
}

fn render_tiers(objs: &[Obj], grand: u64, opts: &Options) {
	let (mut current, mut version, mut historical) = (0u64, 0u64, 0u64);
	for o in objs {
		current += o.p.current;
		version += o.p.version;
		historical += o.p.historical;
	}
	let tiers = [("current", current), ("version-index", version), ("historical", historical)];

	if opts.json {
		for (name, bytes) in tiers {
			print_json(&[("tier", json_str(name)), ("bytes", bytes.to_string())]);
		}
		return;
	}

	let rows: Vec<Vec<String>> = tiers
		.iter()
		.map(|(name, bytes)| vec![fmt_bytes(*bytes), pct(*bytes, grand), name.to_string()])
		.collect();
	table(&["BYTES", "%", "TIER"], &rows);
	println!("\ngrand total: {}", fmt_bytes(grand));
}

fn parent_ns(logical: &str) -> String {
	let base = logical.split("  [").next().unwrap_or(logical).trim_end();
	match base.rfind("::") {
		Some(i) => base[..i].to_string(),
		None => base.to_string(),
	}
}

fn footer(grand: u64, total: usize, shown: usize) {
	if shown < total {
		println!("\n{} of {} objects shown ({} hidden; use --all)", shown, total, total - shown);
	}
	println!("grand total: {} across {} objects", fmt_bytes(grand), total);
}

fn pct(part: u64, whole: u64) -> String {
	if whole == 0 {
		"0.0%".to_string()
	} else {
		format!("{:.1}%", part as f64 * 100.0 / whole as f64)
	}
}

fn fmt_rows(p: &dbstat::Phys) -> String {
	let s = group_int(p.rows);
	if p.rows_exact {
		s
	} else {
		format!("~{s}")
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
		"  {} changes, {} system changes, {} empty commits, {} rollup entries",
		group_int(s.changes),
		group_int(s.system_changes),
		group_int(s.empty_commits),
		group_int(s.rollup_entries)
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

	let payload_raw = s.change_bytes + s.system_bytes;
	println!("\n## user changes vs system changes (raw postcard bytes inside payload)");
	table(
		&["RAW", "%", "COUNT", "STREAM"],
		&[
			vec![
				fmt_bytes(s.change_bytes),
				pct(s.change_bytes, payload_raw),
				group_int(s.changes),
				"changes".to_string(),
			],
			vec![
				fmt_bytes(s.system_bytes),
				pct(s.system_bytes, payload_raw),
				group_int(s.system_changes),
				"system_changes".to_string(),
			],
		],
	);

	println!("\n## by change kind");
	let mut kinds: Vec<(&&str, &cdc::Slice)> = s.diff_kinds.iter().collect();
	kinds.sort_by(|a, b| b.1.bytes.cmp(&a.1.bytes));
	let kind_total: u64 = s.diff_kinds.values().map(|v| v.bytes).sum();
	table(
		&["RAW", "%", "DIFFS", "ROWS", "KIND"],
		&kinds.iter()
			.map(|(k, v)| {
				vec![
					fmt_bytes(v.bytes),
					pct(v.bytes, kind_total),
					group_int(v.count),
					group_int(v.rows),
					k.to_string(),
				]
			})
			.collect::<Vec<_>>(),
	);

	println!("\n## by source object");
	let mut origins: Vec<(&cdc::Origin, &cdc::Slice)> = s.origins.iter().collect();
	origins.sort_by(|a, b| b.1.bytes.cmp(&a.1.bytes));
	let shown = if opts.all {
		origins.len()
	} else {
		opts.top.min(origins.len())
	};
	table(
		&["RAW", "%", "CHANGES", "ROWS", "ORIGIN", "LOGICAL"],
		&origins[..shown]
			.iter()
			.map(|(o, v)| {
				vec![
					fmt_bytes(v.bytes),
					pct(v.bytes, s.change_bytes),
					group_int(v.count),
					group_int(v.rows),
					format!("{}_{}", o.kind, o.id),
					origin_label(cat, o),
				]
			})
			.collect::<Vec<_>>(),
	);
	if shown < origins.len() {
		println!("\n{} of {} origins shown (use --all)", shown, origins.len());
	}

	println!("\n## overlap: same object seen from both streams");
	let storage_of = |o: &cdc::Origin| -> u64 {
		match cat.and_then(|c| c.view_storage.get(&o.id)) {
			Some(under) => *under,
			None => o.id,
		}
	};
	let mut ids: Vec<u64> = s.storage_rows.keys().copied().collect();
	for o in s.origins.keys() {
		let id = storage_of(o);
		if o.kind != "operator" && !ids.contains(&id) {
			ids.push(id);
		}
	}
	let mut overlap: Vec<(u64, u64, u64, u64)> = ids
		.into_iter()
		.map(|id| {
			let sys = s.storage_rows.get(&id).map(|v| v.count).unwrap_or(0);
			let (chg, bytes) = s
				.origins
				.iter()
				.filter(|(o, _)| o.kind != "operator" && storage_of(o) == id)
				.fold((0, 0), |(r, b), (_, v)| (r + v.rows, b + v.bytes));
			(id, chg, sys, bytes)
		})
		.collect();
	overlap.sort_by(|a, b| b.3.cmp(&a.3));
	let ov_shown = if opts.all {
		overlap.len()
	} else {
		opts.top.min(overlap.len())
	};
	table(
		&["CHANGE ROWS", "SYS ROW KEYS", "DELTA", "ID", "LOGICAL"],
		&overlap[..ov_shown]
			.iter()
			.map(|(id, chg, sys, _)| {
				vec![
					group_int(*chg),
					group_int(*sys),
					delta(*chg, *sys),
					id.to_string(),
					storage_label(cat, *id),
				]
			})
			.collect::<Vec<_>>(),
	);
	let (tc, ts): (u64, u64) = overlap.iter().fold((0, 0), |(a, b), (_, c, s, _)| (a + c, b + s));
	println!(
		"totals: {} change rows vs {} system row keys ({} objects, {} undecodable row keys)",
		group_int(tc),
		group_int(ts),
		overlap.len(),
		group_int(s.undecodable_row_keys)
	);
	if ov_shown < overlap.len() {
		println!("{} of {} objects shown (use --all)", ov_shown, overlap.len());
	}

	println!("\n## system changes by key kind");
	let mut sys: Vec<(&String, &cdc::Slice)> = s.system_kinds.iter().collect();
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
				vec![
					fmt_bytes(v.bytes),
					pct(v.bytes, s.system_bytes),
					group_int(v.count),
					k.to_string(),
				]
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
		("change_bytes", s.change_bytes.to_string()),
		("system_changes", s.system_changes.to_string()),
		("system_bytes", s.system_bytes.to_string()),
		("empty_commits", s.empty_commits.to_string()),
		("decode_failures", s.decode_failures.to_string()),
	]);
	for (kind, v) in &s.diff_kinds {
		print_json(&[
			("record", json_str("diff_kind")),
			("kind", json_str(kind)),
			("diffs", v.count.to_string()),
			("rows", v.rows.to_string()),
			("raw_bytes", v.bytes.to_string()),
		]);
	}
	for (o, v) in &s.origins {
		print_json(&[
			("record", json_str("origin")),
			("origin", json_str(&format!("{}_{}", o.kind, o.id))),
			("logical", json_str(&origin_label(cat, o))),
			("changes", v.count.to_string()),
			("rows", v.rows.to_string()),
			("raw_bytes", v.bytes.to_string()),
		]);
	}
	for (kind, v) in &s.system_kinds {
		print_json(&[
			("record", json_str("system_kind")),
			("kind", json_str(kind)),
			("count", v.count.to_string()),
			("raw_bytes", v.bytes.to_string()),
		]);
	}
}

fn origin_label(cat: Option<&Catalog>, o: &cdc::Origin) -> String {
	let Some(cat) = cat else {
		return String::new();
	};
	if o.kind == "operator" {
		return cat
			.operators
			.get(&o.id)
			.map(|(view, stage)| format!("{view}  {stage}"))
			.unwrap_or_else(|| "(unmapped)".to_string());
	}
	if o.kind == "view" {
		if let Some(name) = cat.views.get(&o.id) {
			return format!("{name}  [view]");
		}
	}
	cat.sources.get(&o.id).map(|(name, k)| format!("{name}  [{k}]")).unwrap_or_else(|| "(unmapped)".to_string())
}

fn storage_label(cat: Option<&Catalog>, id: u64) -> String {
	let Some(cat) = cat else {
		return String::new();
	};
	cat.sources.get(&id).map(|(name, k)| format!("{name}  [{k}]")).unwrap_or_else(|| "(unmapped)".to_string())
}

fn delta(a: u64, b: u64) -> String {
	if a == b {
		"0".to_string()
	} else if a > b {
		format!("+{}", group_int(a - b))
	} else {
		format!("-{}", group_int(b - a))
	}
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
