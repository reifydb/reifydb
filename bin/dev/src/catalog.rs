// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{Database, Params, SqliteConfig, Value, embedded};

pub struct Catalog {
	pub sources: HashMap<u64, (String, &'static str)>,
	pub operators: HashMap<u64, String>,
}

pub fn with_open<T>(dir: &str, f: impl FnOnce(&Database) -> Result<T, String>) -> Result<T, String> {
	let mut db =
		embedded::sqlite(SqliteConfig::new(dir)).build().map_err(|e| format!("failed to open '{dir}': {e}"))?;
	let result = f(&db);
	let _ = db.stop();
	result
}

pub fn load(db: &Database) -> Result<Catalog, String> {
	let mut namespaces: HashMap<u64, String> = HashMap::new();
	for r in rows(db, "from system::namespaces")? {
		if let (Some(id), Some(name)) = (u64c(&r, "id"), strc(&r, "name")) {
			namespaces.insert(id, name);
		}
	}

	let mut sources: HashMap<u64, (String, &'static str)> = HashMap::new();
	for (rql, kind) in [
		("from system::tables", "table"),
		("from system::series", "series"),
		("from system::ringbuffers", "ringbuffer"),
	] {
		for r in rows(db, rql)? {
			if let (Some(id), Some(name)) = (u64c(&r, "id"), strc(&r, "name")) {
				sources.insert(id, (qualify(&namespaces, u64c(&r, "namespace_id"), &name), kind));
			}
		}
	}
	for r in rows(db, "from system::views")? {
		if let (Some(under), Some(name)) = (u64c(&r, "underlying_id"), strc(&r, "name")) {
			sources.insert(under, (qualify(&namespaces, u64c(&r, "namespace_id"), &name), "view"));
		}
	}

	let mut flows: HashMap<u64, String> = HashMap::new();
	for r in rows(db, "from system::flows")? {
		if let (Some(id), Some(name)) = (u64c(&r, "id"), strc(&r, "name")) {
			flows.insert(id, qualify(&namespaces, u64c(&r, "namespace_id"), &name));
		}
	}

	let mut operators: HashMap<u64, String> = HashMap::new();
	for r in rows(db, "from system::flow_nodes")? {
		if let Some(id) = u64c(&r, "id") {
			let flow_id = u64c(&r, "flow_id").unwrap_or(0);
			let view = flows.get(&flow_id).cloned().unwrap_or_else(|| format!("flow{flow_id}"));
			operators.insert(id, format!("{view}  {}", operator_label(&blobc(&r, "data"))));
		}
	}

	Ok(Catalog {
		sources,
		operators,
	})
}

fn rows(db: &Database, rql: &str) -> Result<Vec<Vec<(String, Value)>>, String> {
	let frames = db.query_as_root(rql, Params::None).map_err(|e| format!("query `{rql}` failed: {e}"))?;
	Ok(frames.into_iter().flat_map(|f| f.to_rows()).collect())
}

fn cell<'a>(row: &'a [(String, Value)], col: &str) -> Option<&'a Value> {
	row.iter().find(|(n, _)| n == col).map(|(_, v)| v)
}

fn u64c(row: &[(String, Value)], col: &str) -> Option<u64> {
	match cell(row, col)? {
		Value::Uint8(v) => Some(*v),
		Value::Uint4(v) => Some(*v as u64),
		Value::Uint2(v) => Some(*v as u64),
		Value::Uint1(v) => Some(*v as u64),
		Value::Int8(v) if *v >= 0 => Some(*v as u64),
		_ => None,
	}
}

fn strc(row: &[(String, Value)], col: &str) -> Option<String> {
	match cell(row, col)? {
		Value::Utf8(s) => Some(s.clone()),
		_ => None,
	}
}

fn blobc(row: &[(String, Value)], col: &str) -> Vec<u8> {
	match cell(row, col) {
		Some(Value::Blob(b)) => b.as_bytes().to_vec(),
		_ => Vec::new(),
	}
}

fn qualify(namespaces: &HashMap<u64, String>, ns_id: Option<u64>, name: &str) -> String {
	match ns_id.and_then(|id| namespaces.get(&id)) {
		Some(ns) => format!("{ns}::{name}"),
		None => name.to_string(),
	}
}

const NODE_TYPE: &[&str] = &[
	"SourceInlineData",
	"SourceTable",
	"SourceView",
	"SourceFlow",
	"SourceRingBuffer",
	"SourceSeries",
	"Filter",
	"Gate",
	"Map",
	"Extend",
	"Join",
	"Aggregate",
	"Append",
	"Sort",
	"Take",
	"Distinct",
	"Apply",
	"SinkTableView",
];

fn operator_label(data: &[u8]) -> String {
	let Some(&tag) = data.first() else {
		return "[?]".to_string();
	};
	let stage = NODE_TYPE.get(tag as usize).copied().unwrap_or("?");
	if stage == "Apply" {
		let name = first_token(&data[1..]);
		if !name.is_empty() {
			return format!("[{stage}]{{{name}}}");
		}
	}
	format!("[{stage}]")
}

fn first_token(bytes: &[u8]) -> String {
	let mut run = String::new();
	for &b in bytes {
		if (0x20..0x7f).contains(&b) {
			run.push(b as char);
		} else if run.len() >= 3 {
			return run;
		} else {
			run.clear();
		}
	}
	if run.len() >= 3 {
		run
	} else {
		String::new()
	}
}
