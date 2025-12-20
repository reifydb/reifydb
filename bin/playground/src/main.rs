// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{Params, Session, WithSubsystem, embedded};

fn main() {
	// Run the test multiple times to catch flakiness
	for iteration in 0..20 {
		println!("\n==================================================");
		println!("=== ITERATION {} ===", iteration);
		println!("==================================================\n");

		let mut db = embedded::memory().with_worker(|wp| wp).build().unwrap();
		db.start().unwrap();

		// Create namespace and table matching the flaky test
		db.command_as_root("create namespace test", Params::None).unwrap();
		db.command_as_root("create table test.items { id: int4, category: utf8, value: int4 }", Params::None)
			.unwrap();

		// Insert the exact test data
		db.command_as_root(
			r#"
			from [
				{ id: 1, category: "A", value: 30 },
				{ id: 2, category: "B", value: 20 },
				{ id: 3, category: "A", value: 10 },
				{ id: 4, category: "B", value: 40 },
				{ id: 5, category: "A", value: 50 },
				{ id: 6, category: "C", value: 15 },
				{ id: 7, category: "B", value: 25 },
				{ id: 8, category: "C", value: 35 }
			] insert test.items
			"#,
			Params::None,
		)
		.unwrap();

		// Run the flaky query: aggregate | sort | take
		// Expected:
		// - Category A: 90 (30+10+50)
		// - Category B: 85 (20+40+25)
		// - Category C: 50 (15+35)
		// After sort total descending and take 2: A(90), B(85)
		println!("=== Running: aggregate sum + sort + take ===");
		for frame in db
			.query_as_root(
				"from test.items aggregate { total: math::sum(value) } by category sort total take 2",
				Params::None,
			)
			.unwrap()
		{
			println!("{}", frame);
		}

		// Test the COUNT case which has TIES (A=3, B=3, C=2)
		// This is where flakiness is more likely!
		println!("\n=== Running: aggregate count + sort + take (HAS TIES!) ===");
		for frame in db
			.query_as_root(
				"from test.items aggregate { cnt: math::count(id) } by category sort cnt take 2",
				Params::None,
			)
			.unwrap()
		{
			println!("{}", frame);
		}
	}
}
