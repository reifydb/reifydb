// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::catalog::Catalog;

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
