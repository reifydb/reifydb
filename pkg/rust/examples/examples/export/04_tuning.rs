// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Scenario: tuning the output. `batch_size` trades statement count against statement width - 1 per
// row is diff-friendly, a large size imports faster. `if_not_exists(true)` makes the DDL
// re-appliable, and only namespace / table / dictionary / enum support it, hence the `shop` scope.

use reifydb::{ExportOptions, embedded};
use reifydb_examples::seed_demo;

fn main() {
	let db = embedded::memory().build().unwrap();
	seed_demo(&db);

	// batch_size: one INSERT per row vs a single batched INSERT.
	let per_row = db.export(&ExportOptions::all().data_only().batch_size(1)).unwrap();
	let batched = db.export(&ExportOptions::all().data_only().batch_size(1000)).unwrap();

	let per_row_inserts = per_row.matches("INSERT shop::products").count();
	let batched_inserts = batched.matches("INSERT shop::products").count();
	println!("=== batch_size(1): {per_row_inserts} INSERT statements for shop::products ===\n{per_row}");
	println!("=== batch_size(1000): {batched_inserts} INSERT statement for shop::products ===\n{batched}");
	assert_eq!(per_row_inserts, 3, "3 rows at batch_size(1) -> 3 statements");
	assert_eq!(batched_inserts, 1, "3 rows at batch_size(1000) -> 1 statement");

	// if_not_exists: without it, re-applying the schema errors on the second run.
	let plain = db.export(&ExportOptions::all().namespace("shop").schema_only()).unwrap();
	let target_plain = embedded::memory().build().unwrap();
	target_plain.import(&plain).unwrap();
	let replayed = target_plain.import(&plain);
	println!("=== re-importing plain schema a second time -> error: {} ===", replayed.is_err());
	assert!(replayed.is_err(), "plain CREATE statements reject pre-existing objects");

	// With if_not_exists, the same schema applies cleanly any number of times.
	let idempotent = db.export(&ExportOptions::all().namespace("shop").schema_only().if_not_exists(true)).unwrap();
	println!("=== schema_only().if_not_exists(true) ===\n{idempotent}");
	let target_ine = embedded::memory().build().unwrap();
	target_ine.import(&idempotent).unwrap();
	target_ine.import(&idempotent).unwrap();
	println!("=== re-imported IF NOT EXISTS schema twice with no error ===");
}
