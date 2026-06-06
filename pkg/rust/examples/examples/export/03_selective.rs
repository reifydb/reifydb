// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Scenario: narrowing what gets exported.
// Selection has three axes - by namespace, by shape, or by kind. They are
// printed here with `.schema_only()` so the output shows exactly which shapes
// are included. Two behaviors worth seeing:
//   - dependency closure: a selected shape always drags in the dictionaries / enums its columns reference, so the
//     script stays self-contained.
//   - axes do NOT combine: calling a second selection axis REPLACES the first (last-axis-wins), so
//     `.namespace(..).kind(..)` is just `.kind(..)`.

use reifydb::{ExportOptions, ShapeKind, embedded};
use reifydb_examples::seed_demo;

fn main() {
	let db = embedded::memory().build().unwrap();
	seed_demo(&db);

	// By namespace: only the `metrics` shapes (series + ring buffer).
	let by_namespace = db.export(&ExportOptions::all().namespace("metrics").schema_only()).unwrap();
	println!("=== namespace(\"metrics\") ===\n{by_namespace}");
	assert!(by_namespace.contains("CREATE SERIES metrics::events"));
	assert!(by_namespace.contains("CREATE RINGBUFFER metrics::recent"));
	assert!(!by_namespace.contains("shop::products"), "namespace selection must exclude other namespaces");

	// By shape: just one table - but its dictionary- and enum-typed columns pull `shop::tokens`
	// and `shop::status` in automatically (dependency closure), keeping the script self-contained.
	let by_shape = db.export(&ExportOptions::all().shape("shop", "products").schema_only()).unwrap();
	println!("=== shape(\"shop\", \"products\") + dependency closure ===\n{by_shape}");
	assert!(by_shape.contains("CREATE TABLE shop::products"));
	assert!(by_shape.contains("CREATE DICTIONARY shop::tokens"), "referenced dictionary must be included");
	assert!(by_shape.contains("CREATE ENUM shop::status"), "referenced enum must be included");

	// By kind: all reference data (every dictionary + enum), no tables/series.
	let by_kind = db
		.export(&ExportOptions::all().kind(ShapeKind::Dictionary).kind(ShapeKind::Enum).schema_only())
		.unwrap();
	println!("=== kind(Dictionary) + kind(Enum) ===\n{by_kind}");
	assert!(by_kind.contains("CREATE DICTIONARY shop::tokens"));
	assert!(by_kind.contains("CREATE ENUM shop::status"));
	assert!(!by_kind.contains("CREATE TABLE"), "kind selection must exclude tables");

	// Gotcha: `.kind()` overwrites the earlier `.namespace()`. Despite asking
	// for the `metrics` namespace, this exports every TABLE (shop::products),
	// because the last selection axis wins.
	let overwritten =
		db.export(&ExportOptions::all().namespace("metrics").kind(ShapeKind::Table).schema_only()).unwrap();
	println!("=== namespace(\"metrics\").kind(Table) -> kind wins ===\n{overwritten}");
	assert!(overwritten.contains("CREATE TABLE shop::products"), "kind(Table) replaced the namespace filter");
	assert!(!overwritten.contains("metrics::events"), "the namespace filter was discarded");
}
