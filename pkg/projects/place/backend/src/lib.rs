// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, env::var_os, path::PathBuf};

use reifydb::{Database, IdentityId, Value, value::params::Params};

pub fn migration_path() -> PathBuf {
	var_os("MIGRATION_PATH")
		.map(PathBuf::from)
		.unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migration")))
}

fn named(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

pub fn seed(db: &Database) -> Result<(), String> {
	for tx in 0..32u8 {
		for ty in 0..32u8 {
			match tile_rows(db, tx, ty)? {
				4096 => continue,
				0 => insert_tile(db, tx, ty)?,
				other => return Err(format!("tile ({tx},{ty}) has {other} rows, expected 0 or 4096")),
			}
		}
	}
	mark_ready(db)
}

fn mark_ready(db: &Database) -> Result<(), String> {
	let frames = db
		.query_as_root(
			"from place::state filter { key == \"canvas\" } aggregate { n: math::count(key) }",
			Params::None,
		)
		.map_err(|e| format!("read canvas state: {e:?}"))?;
	let marked = match frames.first().and_then(|f| f.rows().next()) {
		None => 0,
		Some(row) => row.get::<i64>("n").map_err(|e| format!("read canvas state count: {e:?}"))?.unwrap_or(0),
	};
	if marked > 0 {
		return Ok(());
	}
	db.command_as_root("insert place::state [{ key: \"canvas\", ready: true }]", Params::None)
		.map(|_| ())
		.map_err(|e| format!("mark canvas ready: {e:?}"))
}

fn tile_rows(db: &Database, tx: u8, ty: u8) -> Result<i64, String> {
	let frames = db
		.query_as_root(
			"from place::live filter { tile_x == $tx and tile_y == $ty } aggregate { n: math::count(x) }",
			named(&[("tx", Value::Uint1(tx)), ("ty", Value::Uint1(ty))]),
		)
		.map_err(|e| format!("count tile ({tx},{ty}): {e:?}"))?;
	match frames.first().and_then(|f| f.rows().next()) {
		None => Ok(0),
		Some(row) => row
			.get::<i64>("n")
			.map_err(|e| format!("read count of tile ({tx},{ty}): {e:?}"))
			.map(|n| n.unwrap_or(0)),
	}
}

fn insert_tile(db: &Database, tx: u8, ty: u8) -> Result<(), String> {
	let (bx, by) = (tx as u16 * 64, ty as u16 * 64);
	let rows = (0..64u16).flat_map(move |dy| {
		(0..64u16).map(move |dx| {
			named(&[
				("tile_x", Value::Uint1(tx)),
				("tile_y", Value::Uint1(ty)),
				("x", Value::Uint2(bx + dx)),
				("y", Value::Uint2(by + dy)),
				("color", Value::Uint1(0)),
			])
		})
	});
	let mut b = db.engine().bulk_insert(IdentityId::root());
	b.table("place::live").rows(rows).done();
	b.execute().map(|_| ()).map_err(|e| format!("seed tile ({tx},{ty}): {e:?}"))
}
