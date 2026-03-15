// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::{engine::StandardEngine, test_utils::create_test_engine};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

fn setup_schema(engine: &StandardEngine) {
	engine.admin_as(IdentityId::system(), "CREATE NAMESPACE ns", Params::None).unwrap();
	engine.admin_as(IdentityId::system(), "CREATE TABLE ns::t { id: int8, name: utf8 }", Params::None).unwrap();
}

fn query_rows(engine: &StandardEngine) -> Vec<(i64, String)> {
	let frames = engine.query_as(IdentityId::system(), "FROM ns::t", Params::None).unwrap();
	extract_rows(&frames)
}

fn extract_rows(frames: &[Frame]) -> Vec<(i64, String)> {
	let frame = match frames.first() {
		Some(f) => f,
		None => return vec![],
	};
	let mut rows: Vec<_> = frame
		.rows()
		.map(|r| (r.get::<i64>("id").unwrap().unwrap(), r.get::<String>("name").unwrap().unwrap()))
		.collect();
	rows.sort_by_key(|(id, _)| *id);
	rows
}

#[test]
fn test_identity_propagates_to_all_transaction_types() {
	let engine = create_test_engine();
	setup_schema(&engine);

	let identity = IdentityId::system();

	// Admin
	let txn = engine.begin_admin(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Admin(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);

	// Command
	let txn = engine.begin_command(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Command(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);

	// Query
	let txn = engine.begin_query(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Query(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);

	// Subscription
	let txn = engine.begin_subscription(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Subscription(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);
}

#[test]
fn test_rql_select_through_all_transaction_types() {
	let engine = create_test_engine();
	setup_schema(&engine);
	engine.command_as(IdentityId::system(), r#"INSERT ns::t [{ id: 1, name: "alice" }]"#, Params::None).unwrap();

	let expected = vec![(1i64, "alice".to_string())];

	// Admin
	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let frames = txn.rql("FROM ns::t", Params::None).unwrap();
	assert_eq!(extract_rows(&frames), expected);
	drop(txn);

	// Command
	let mut txn = engine.begin_command(IdentityId::system()).unwrap();
	let frames = txn.rql("FROM ns::t", Params::None).unwrap();
	assert_eq!(extract_rows(&frames), expected);
	drop(txn);

	// Query
	let mut txn = engine.begin_query(IdentityId::system()).unwrap();
	let frames = txn.rql("FROM ns::t", Params::None).unwrap();
	assert_eq!(extract_rows(&frames), expected);
	drop(txn);

	// Subscription
	let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
	let frames = txn.rql("FROM ns::t", Params::None).unwrap();
	assert_eq!(extract_rows(&frames), expected);
	drop(txn);
}

#[test]
fn test_rql_insert_and_commit() {
	// Admin
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
		txn.rql(r#"INSERT ns::t [{ id: 1, name: "alice" }]"#, Params::None).unwrap();
		txn.commit().unwrap();
		assert_eq!(query_rows(&engine), vec![(1, "alice".to_string())]);
	}

	// Command
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		txn.rql(r#"INSERT ns::t [{ id: 2, name: "bob" }]"#, Params::None).unwrap();
		txn.commit().unwrap();
		assert_eq!(query_rows(&engine), vec![(2, "bob".to_string())]);
	}

	// Subscription
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
		txn.rql(r#"INSERT ns::t [{ id: 3, name: "charlie" }]"#, Params::None).unwrap();
		txn.commit().unwrap();
		assert_eq!(query_rows(&engine), vec![(3, "charlie".to_string())]);
	}
}

#[test]
fn test_rql_error_poisons_transaction() {
	for label in ["admin", "command", "subscription"] {
		let engine = create_test_engine();
		setup_schema(&engine);

		match label {
			"admin" => {
				let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
				txn.rql(r#"INSERT ns::t [{ id: 1, name: "row1" }]"#, Params::None).unwrap();
				let err = txn.rql(r#"INSERT nonexistent::table [{ id: 1 }]"#, Params::None);
				assert!(err.is_err());
				assert!(txn.commit().is_err(), "commit after poison must fail for {label}");
			}
			"command" => {
				let mut txn = engine.begin_command(IdentityId::system()).unwrap();
				txn.rql(r#"INSERT ns::t [{ id: 1, name: "row1" }]"#, Params::None).unwrap();
				let err = txn.rql(r#"INSERT nonexistent::table [{ id: 1 }]"#, Params::None);
				assert!(err.is_err());
				assert!(txn.commit().is_err(), "commit after poison must fail for {label}");
			}
			"subscription" => {
				let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
				txn.rql(r#"INSERT ns::t [{ id: 1, name: "row1" }]"#, Params::None).unwrap();
				let err = txn.rql(r#"INSERT nonexistent::table [{ id: 1 }]"#, Params::None);
				assert!(err.is_err());
				assert!(txn.commit().is_err(), "commit after poison must fail for {label}");
			}
			_ => unreachable!(),
		}

		assert_eq!(query_rows(&engine), vec![], "poisoned transaction must not persist rows for {label}");
	}
}

#[test]
fn test_drop_without_commit_rolls_back() {
	// Admin
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
		txn.rql(r#"INSERT ns::t [{ id: 1, name: "alice" }]"#, Params::None).unwrap();
		drop(txn);
		assert_eq!(query_rows(&engine), vec![]);
	}

	// Command
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		txn.rql(r#"INSERT ns::t [{ id: 1, name: "bob" }]"#, Params::None).unwrap();
		drop(txn);
		assert_eq!(query_rows(&engine), vec![]);
	}

	// Subscription
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
		txn.rql(r#"INSERT ns::t [{ id: 1, name: "charlie" }]"#, Params::None).unwrap();
		drop(txn);
		assert_eq!(query_rows(&engine), vec![]);
	}
}

#[test]
fn test_rql_after_commit_errors() {
	// Admin
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
		txn.commit().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Command
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		txn.commit().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Subscription
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
		txn.commit().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}
}

#[test]
fn test_rql_after_rollback_errors() {
	// Admin
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
		txn.rollback().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Command
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		txn.rollback().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Subscription
	{
		let engine = create_test_engine();
		setup_schema(&engine);
		let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
		txn.rollback().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}
}

#[test]
fn test_rql_syntax_error_returns_err() {
	let engine = create_test_engine();
	setup_schema(&engine);

	// Admin
	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);

	// Command
	let mut txn = engine.begin_command(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);

	// Query
	let mut txn = engine.begin_query(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);

	// Subscription
	let mut txn = engine.begin_subscription(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);
}

#[test]
fn test_transaction_enum_rql() {
	let engine = create_test_engine();
	setup_schema(&engine);
	engine.command_as(IdentityId::system(), r#"INSERT ns::t [{ id: 1, name: "alice" }]"#, Params::None).unwrap();

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let mut tx = Transaction::Admin(&mut txn);
	let frames = tx.rql("FROM ns::t", Params::None).unwrap();
	assert_eq!(extract_rows(&frames), vec![(1i64, "alice".to_string())]);
}
