// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Scenario: two-phase migration (schema first, then data).
// `.schema_only()` emits just the CREATE statements; `.data_only()` emits just
// the INSERT statements (and assumes the schema already exists in the target).
// Splitting the two lets you stand up (and optionally tweak) the structure
// before loading any rows.

use reifydb::{ExportOptions, Params, embedded};
use reifydb_examples::seed_demo;
use tracing::info;

fn main() {
	let source = embedded::memory().build().unwrap();
	seed_demo(&source);

	// Phase 1: structure only - no INSERTs.
	let schema = source.export(&ExportOptions::all().schema_only()).unwrap();
	println!("=== schema_only (DDL, no data) ===\n{schema}");
	assert!(schema.contains("CREATE TABLE shop::products"));
	assert!(!schema.contains("INSERT"), "schema_only must not contain data");

	// Phase 2: data only - no DDL. The target must already have the shapes.
	let data = source.export(&ExportOptions::all().data_only()).unwrap();
	println!("=== data_only (INSERTs, no DDL) ===\n{data}");
	assert!(data.contains("INSERT shop::products"));
	assert!(!data.contains("CREATE"), "data_only must not contain DDL");

	// Apply them in order to a fresh database: schema, then data.
	let target = embedded::memory().build().unwrap();
	target.import(&schema).unwrap();
	info!("Applied schema to the target");
	target.import(&data).unwrap();
	info!("Loaded data into the target");

	let count = target
		.query_as_root("from shop::products", Params::None)
		.unwrap()
		.into_iter()
		.flat_map(|f| f.to_rows())
		.count();
	println!("=== target now has {count} rows in shop::products ===");
	assert_eq!(count, 3);
}
