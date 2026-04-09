// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb::{Database, server};
use reifydb_type::params::Params;

fn admin(db: &Database, label: &str, cmd: &str) {
	println!("\n--- {label} ---");
	println!("> {cmd}");
	match db.admin_as_root(cmd, Params::None) {
		Ok(frames) => {
			for frame in &frames {
				println!("{frame}");
			}
		}
		Err(e) => println!("ERROR: {e}"),
	}
}

fn query(db: &Database, label: &str, cmd: &str) {
	println!("\n--- {label} ---");
	println!("> {cmd}");
	match db.query_as_root(cmd, Params::None) {
		Ok(frames) => {
			for frame in &frames {
				println!("{frame}");
			}
		}
		Err(e) => println!("ERROR: {e}"),
	}
}

fn main() {
	let db = server::memory().build().unwrap();

	// ── Shape ──────────────────────────────────────────────
	admin(&db, "Create namespace", "CREATE NAMESPACE demo");
	admin(&db, "Create table", "CREATE TABLE demo::users { id: Int4, name: Text, active: Boolean }");
	admin(
		&db,
		"Insert seed data",
		r#"INSERT demo::users [
			{ id: 1, name: "Alice",   active: true  },
			{ id: 2, name: "Bob",     active: true  },
			{ id: 3, name: "Charlie", active: false }
		]"#,
	);

	// ── First-class tests ───────────────────────────────────
	admin(
		&db,
		"CREATE TEST — checks Alice is present",
		r#"CREATE TEST demo::alice_exists {
			FROM demo::users | FILTER name == "Alice" | ASSERT { name == "Alice" }
		}"#,
	);

	admin(
		&db,
		"CREATE TEST — checks Bob is active",
		r#"CREATE TEST demo::bob_active {
			FROM demo::users | FILTER name == "Bob" | ASSERT { active == true }
		}"#,
	);

	admin(
		&db,
		"CREATE TEST — deliberately failing (Charlie is inactive)",
		r#"CREATE TEST demo::charlie_is_active {
			FROM demo::users | FILTER name == "Charlie" | ASSERT { active == true }
		}"#,
	);

	// ── Run tests ───────────────────────────────────────────
	admin(&db, "RUN TESTS demo (all in namespace)", "RUN TESTS demo");

	admin(&db, "RUN TEST (single passing)", "RUN TEST demo::alice_exists");

	admin(&db, "RUN TEST (single failing)", "RUN TEST demo::charlie_is_active");

	// ── Tests with mutations ────────────────────────────────
	admin(
		&db,
		"CREATE TEST — inserts inside test body",
		r#"CREATE TEST demo::insert_in_test {
			INSERT demo::users [{ id: 99, name: "Ghost", active: true }];
			FROM demo::users | FILTER id == 99 | ASSERT { name == "Ghost" }
		}"#,
	);

	admin(&db, "Run the insert test", "RUN TEST demo::insert_in_test");

	query(&db, "Verify Ghost row exists (no per-test rollback yet)", "FROM demo::users | FILTER id == 99");

	// ── Run all tests ───────────────────────────────────────
	admin(&db, "RUN TESTS (all tests in database)", "RUN TESTS");
}
