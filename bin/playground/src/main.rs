// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use std::thread::sleep;

use reifydb::{
	Database, Frame, IdentityId, Result, allocator, server,
	value::{params::Params, value::duration::Duration},
};

allocator::set_global_allocator!();

const POLL_ATTEMPTS: usize = 100;

fn show(label: &str, rql: &str, outcome: Result<Vec<Frame>>) {
	println!("\n--- {label} ---");
	println!("> {rql}");
	match outcome {
		Ok(frames) => {
			for frame in &frames {
				println!("{frame}");
			}
		}
		Err(e) => println!("ERROR: {e}"),
	}
}

fn column_result(db: &Database, rql: &str) -> Result<Vec<Frame>> {
	let result = db.engine().query_column_as(IdentityId::root(), rql, Params::None);
	match result.error {
		Some(e) => Err(e),
		None => Ok(result.frames),
	}
}

fn admin(db: &Database, label: &str, rql: &str) {
	show(label, rql, db.admin_as_root(rql, Params::None));
}

fn command(db: &Database, label: &str, rql: &str) {
	show(label, rql, db.command_as_root(rql, Params::None));
}

fn column_query(db: &Database, label: &str, rql: &str) {
	show(label, rql, column_result(db, rql));
}

fn count_rows(frames: &[Frame]) -> usize {
	frames.iter().map(|frame| frame.row_count()).sum()
}

fn await_column_rows(db: &Database, label: &str, rql: &str, want: usize) {
	println!("\n--- {label} ---");
	println!("> {rql}");
	for attempt in 1..=POLL_ATTEMPTS {
		if let Ok(frames) = column_result(db, rql)
			&& count_rows(&frames) == want
		{
			println!("column store holds {want} rows after {attempt} polls");
			return;
		}
		sleep(Duration::from_milliseconds(100).unwrap().to_std());
	}
	panic!("column store did not reach {want} rows for `{rql}` within {POLL_ATTEMPTS} polls");
}

fn main() {
	allocator::verify();

	let db = server::memory().build().unwrap();

	admin(&db, "1. Create namespace", "CREATE NAMESPACE demo");
	admin(&db, "1. Create table", "CREATE TABLE demo::users { id: Int4, name: Text, active: Boolean }");
	command(
		&db,
		"1. Insert seed data (5 rows)",
		r#"INSERT demo::users [
			{ id: 1, name: "Alice",   active: true  },
			{ id: 2, name: "Bob",     active: true  },
			{ id: 3, name: "Charlie", active: false },
			{ id: 4, name: "Dana",    active: true  },
			{ id: 5, name: "Eve",     active: false }
		]"#,
	);

	column_query(&db, "2. Column query before the table is materialized (expect QUERY_012)", "FROM demo::users");

	await_column_rows(&db, "3. Wait for the column snapshot", "FROM demo::users", 5);

	column_query(&db, "4. Column query (rows plus #commit_version)", "FROM demo::users");

	column_query(
		&db,
		"5. Filter and map run on top of the column scan",
		"FROM demo::users | FILTER active == true | MAP { id, name }",
	);

	command(
		&db,
		"6. Insert 2 more rows",
		r#"INSERT demo::users [
			{ id: 6, name: "Frank", active: true },
			{ id: 7, name: "Grace", active: true }
		]"#,
	);
	column_query(&db, "6. Column query right after the insert (serves the last snapshot)", "FROM demo::users");
	await_column_rows(&db, "6. Wait for the next snapshot", "FROM demo::users", 7);
	column_query(&db, "6. Column query after the next snapshot", "FROM demo::users");
}
