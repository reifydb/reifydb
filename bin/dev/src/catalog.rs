// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{Database, Params, SqliteConfig, Value, embedded};

pub struct Catalog {
	pub sources: HashMap<u64, (String, &'static str)>,
	pub views: HashMap<u64, String>,
	pub operators: HashMap<u64, (String, String)>,
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
	let mut views: HashMap<u64, String> = HashMap::new();
	for r in rows(db, "from system::views")? {
		let (Some(id), Some(name)) = (u64c(&r, "id"), strc(&r, "name")) else {
			continue;
		};
		let qualified = qualify(&namespaces, u64c(&r, "namespace_id"), &name);
		sources.insert(id, (qualified.clone(), "view"));
		views.insert(id, qualified);
	}

	let mut flows: HashMap<u64, String> = HashMap::new();
	for r in rows(db, "from system::flows")? {
		if let (Some(id), Some(name)) = (u64c(&r, "id"), strc(&r, "name")) {
			flows.insert(id, qualify(&namespaces, u64c(&r, "namespace_id"), &name));
		}
	}

	let mut operators: HashMap<u64, (String, String)> = HashMap::new();
	for r in rows(db, "from system::flow::operators")? {
		if let Some(id) = u64c(&r, "id") {
			let flow_id = u64c(&r, "flow_id").unwrap_or(0);
			let view = flows.get(&flow_id).cloned().unwrap_or_else(|| format!("flow{flow_id}"));
			let kind = strc(&r, "kind").ok_or("system::flow::operators row has no kind")?;
			operators.insert(id, (view, format!("[{kind}]")));
		}
	}

	Ok(Catalog {
		sources,
		views,
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

fn qualify(namespaces: &HashMap<u64, String>, ns_id: Option<u64>, name: &str) -> String {
	match ns_id.and_then(|id| namespaces.get(&id)) {
		Some(ns) => format!("{ns}::{name}"),
		None => name.to_string(),
	}
}
