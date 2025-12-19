// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::{Params, Session, WithSubsystem, embedded};

fn main() {
	let mut db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.with_flow(|f| f)
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace
	db.command_as_root(r#"create namespace test"#, Params::None).unwrap();

	// Create dictionary
	db.command_as_root(r#"create dictionary test.colors for utf8 as uint2"#, Params::None).unwrap();

	println!("\n=== Dictionary storage stats (immediately after creation) ===");
	for frame in db.query_as_root(r#"from system.dictionary_storage_stats"#, Params::None).unwrap() {
		println!("{}", frame);
	}
}
