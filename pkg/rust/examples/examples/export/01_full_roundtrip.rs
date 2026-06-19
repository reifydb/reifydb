// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Scenario: full logical backup / clone.
// `ExportOptions::all()` dumps the schema AND data of every user shape into a
// single, self-contained RQL script. Re-importing that script into a fresh
// database reproduces the original exactly.

use reifydb::{Database, ExportOptions, Params, Value, embedded};
use reifydb_examples::seed_demo;
use tracing::info;

fn rows(db: &Database, query: &str) -> Vec<Vec<(String, Value)>> {
	db.query_as_root(query, Params::None).unwrap().into_iter().flat_map(|f| f.to_rows()).collect()
}

// Order rows by debug form so two result sets can be compared regardless of scan
// order; equality still uses the database's own `Value` notion of "same data".
fn sorted(mut rows: Vec<Vec<(String, Value)>>) -> Vec<Vec<(String, Value)>> {
	rows.sort_by_key(|row| format!("{row:?}"));
	rows
}

fn main() {
	// Source database with the shared demo schema + data.
	let source = embedded::memory().build().unwrap();
	seed_demo(&source);

	// Export everything: CREATE statements followed by INSERT batches.
	let dump = source.export(&ExportOptions::all()).unwrap();
	println!("=== Full export (ExportOptions::all) ===\n{dump}");

	// Re-materialize the dump into a brand-new, empty database.
	let restored = embedded::memory().build().unwrap();
	restored.import(&dump).unwrap();
	info!("Imported the dump into a fresh database");

	// Prove the round-trip preserved the data: the same query returns the
	// same rows from both databases.
	for shape in ["shop::products", "metrics::events", "metrics::recent"] {
		let query = format!("from {shape}");
		let from_source = rows(&source, &query);
		let from_restored = rows(&restored, &query);

		println!(
			"=== {shape}: {} rows in source, {} rows restored ===",
			from_source.len(),
			from_restored.len()
		);
		assert_eq!(sorted(from_source), sorted(from_restored), "round-trip changed the rows of {shape}");
	}

	info!("Round-trip verified: restored database matches the source");
}
