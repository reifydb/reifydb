// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use crate::{catalog::Catalog, dbstat};

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
				_ => cat.operators.get(id).cloned().unwrap_or_else(|| "(unmapped)".to_string()),
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

pub fn dump_catalog(cat: &Catalog, json: bool) {
	let mut sources: Vec<(&u64, &(String, &str))> = cat.sources.iter().collect();
	sources.sort_by_key(|(id, _)| **id);
	let mut operators: Vec<(&u64, &String)> = cat.operators.iter().collect();
	operators.sort_by_key(|(id, _)| **id);

	if json {
		for (id, (name, kind)) in &sources {
			print_json(&[
				("source_id", id.to_string()),
				("name", json_str(name)),
				("kind", json_str(kind)),
			]);
		}
		for (id, label) in &operators {
			print_json(&[("operator_id", id.to_string()), ("view", json_str(label))]);
		}
		return;
	}

	println!("# {} sources, {} flow-node operators\n", sources.len(), operators.len());
	println!("## source_<id> -> name");
	for (id, (name, kind)) in &sources {
		println!("  source_{:<6} {}  [{}]", id, name, kind);
	}
	println!("\n## operator_<id> -> view [stage]{{operator}}");
	for (id, label) in &operators {
		println!("  operator_{:<5} {}", id, label);
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
