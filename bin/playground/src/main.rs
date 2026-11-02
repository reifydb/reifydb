// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb::{
	Database, Frame, IdentityId, Result, allocator, server, transaction::transaction::command::CommandTransaction,
	value::params::Params,
};

allocator::set_global_allocator!();

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

fn admin(db: &Database, label: &str, rql: &str) {
	show(label, rql, db.admin_as_root(rql, Params::None));
}

fn command(db: &Database, label: &str, rql: &str) {
	show(label, rql, db.command_as_root(rql, Params::None));
}

fn query(db: &Database, label: &str, rql: &str) {
	show(label, rql, db.query_as_root(rql, Params::None));
}

fn in_txn(txn: &mut CommandTransaction, label: &str, rql: &str) {
	let result = txn.rql(rql, Params::None);
	let outcome = match result.error {
		Some(e) => Err(e),
		None => Ok(result.frames),
	};
	show(label, rql, outcome);
}

fn main() {
	allocator::verify();

	let db = server::memory().build().unwrap();

	admin(&db, "1. Create namespace", "CREATE NAMESPACE shop");
	admin(&db, "1. Create table", "CREATE TABLE shop::orders { id: Int4, customer: Utf8, amount: Int4 }");
	admin(
		&db,
		"1. Create transactional view (orders above 100)",
		"CREATE TRANSACTIONAL VIEW shop::big_orders { id: Int4, customer: Utf8, amount: Int4 } AS { FROM shop::orders | FILTER { amount > 100 } }",
	);

	command(
		&db,
		"2. Insert 4 orders",
		r#"INSERT shop::orders [
			{ id: 1, customer: "Alice", amount: 50  },
			{ id: 2, customer: "Bob",   amount: 150 },
			{ id: 3, customer: "Carol", amount: 300 },
			{ id: 4, customer: "Dana",  amount: 90  }
		]"#,
	);
	query(&db, "2. View right after the insert (expect 2 and 3, no lag)", "FROM shop::big_orders");

	command(
		&db,
		"3. Update Alice to 500 (moves into the view)",
		"UPDATE shop::orders { amount: 500 } FILTER { id == 1 }",
	);
	command(
		&db,
		"3. Update Bob to 10 (moves out of the view)",
		"UPDATE shop::orders { amount: 10 } FILTER { id == 2 }",
	);
	query(&db, "3. View after the updates (expect 1 and 3)", "FROM shop::big_orders");

	command(&db, "4. Delete Carol", "DELETE shop::orders FILTER { id == 3 }");
	query(&db, "4. View after the delete (expect 1)", "FROM shop::big_orders");

	let mut txn = db.engine().begin_command(IdentityId::root()).unwrap();
	in_txn(
		&mut txn,
		"5. In one txn: insert Erin 200",
		r#"INSERT shop::orders [{ id: 5, customer: "Erin", amount: 200 }]"#,
	);
	in_txn(&mut txn, "5. Same txn: read the view (expect 1 and 5)", "FROM shop::big_orders");
	txn.commit().unwrap();
	query(&db, "5. After commit (expect 1 and 5)", "FROM shop::big_orders");

	let mut txn = db.engine().begin_command(IdentityId::root()).unwrap();
	in_txn(
		&mut txn,
		"6. In one txn: insert Frank 999",
		r#"INSERT shop::orders [{ id: 6, customer: "Frank", amount: 999 }]"#,
	);
	in_txn(&mut txn, "6. Same txn: read the view (expect 1, 5 and 6)", "FROM shop::big_orders");
	txn.rollback().unwrap();
	query(&db, "6. After rollback (expect 1 and 5, Frank is gone)", "FROM shop::big_orders");

	admin(
		&db,
		"7. Create a test that writes and reads the view",
		r#"CREATE TEST shop::big_order_is_visible {
			INSERT shop::orders [{ id: 7, customer: "Gina", amount: 700 }];
			FROM shop::big_orders | FILTER { id == 7 } | AGGREGATE { n: math::count(id) } | ASSERT { n == 1 }
		}"#,
	);
	admin(&db, "7. Run the tests (expect pass)", "RUN TESTS shop");
	query(&db, "7. View after RUN TESTS (expect 1 and 5, the test write is gone)", "FROM shop::big_orders");
}
