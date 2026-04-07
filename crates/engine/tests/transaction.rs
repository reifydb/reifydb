// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

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
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");

	let identity = IdentityId::system();

	// Admin
	let txn = t.begin_admin(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Admin(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);

	// Command
	let txn = t.begin_command(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Command(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);

	// Query
	let txn = t.begin_query(identity).unwrap();
	assert_eq!(txn.identity, identity);
	let mut txn = txn;
	let tx = Transaction::Query(&mut txn);
	assert_eq!(tx.identity(), identity);
	drop(txn);
}

#[test]
fn test_rql_select_through_all_transaction_types() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
	t.command(r#"INSERT ns::t [{ id: 1, name: "alice" }]"#);

	let expected = vec![(1i64, "alice".to_string())];

	// Admin
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("FROM ns::t", Params::None);
	if let Some(e) = r.error {
		panic!("{e:?}");
	}
	assert_eq!(extract_rows(&r), expected);
	drop(txn);

	// Command
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("FROM ns::t", Params::None);
	if let Some(e) = r.error {
		panic!("{e:?}");
	}
	assert_eq!(extract_rows(&r), expected);
	drop(txn);

	// Query
	let mut txn = t.begin_query(IdentityId::system()).unwrap();
	let r = txn.rql("FROM ns::t", Params::None);
	if let Some(e) = r.error {
		panic!("{e:?}");
	}
	assert_eq!(extract_rows(&r), expected);
	drop(txn);

	// (Subscription testing covered by admin above)
}

#[test]
fn test_rql_insert_and_commit() {
	// Admin
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "alice" }]"#, Params::None);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		txn.commit().unwrap();
		assert_eq!(extract_rows(&t.query("FROM ns::t")), vec![(1, "alice".to_string())]);
	}

	// Command
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_command(IdentityId::system()).unwrap();
		let r = txn.rql(r#"INSERT ns::t [{ id: 2, name: "bob" }]"#, Params::None);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		txn.commit().unwrap();
		assert_eq!(extract_rows(&t.query("FROM ns::t")), vec![(2, "bob".to_string())]);
	}

	// Admin (Subscription removed)
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		let r = txn.rql(r#"INSERT ns::t [{ id: 3, name: "charlie" }]"#, Params::None);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		txn.commit().unwrap();
		assert_eq!(extract_rows(&t.query("FROM ns::t")), vec![(3, "charlie".to_string())]);
	}
}

#[test]
fn test_rql_error_poisons_transaction() {
	for label in ["admin", "command", "subscription"] {
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");

		match label {
			"admin" => {
				let mut txn = t.begin_admin(IdentityId::system()).unwrap();
				let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "row1" }]"#, Params::None);
				if let Some(e) = r.error {
					panic!("{e:?}");
				}
				let r = txn.rql(r#"INSERT nonexistent::table [{ id: 1 }]"#, Params::None);
				assert!(r.is_err());
				assert!(txn.commit().is_err(), "commit after poison must fail for {label}");
			}
			"command" => {
				let mut txn = t.begin_command(IdentityId::system()).unwrap();
				let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "row1" }]"#, Params::None);
				if let Some(e) = r.error {
					panic!("{e:?}");
				}
				let r = txn.rql(r#"INSERT nonexistent::table [{ id: 1 }]"#, Params::None);
				assert!(r.is_err());
				assert!(txn.commit().is_err(), "commit after poison must fail for {label}");
			}
			"subscription" => {
				let mut txn = t.begin_admin(IdentityId::system()).unwrap();
				let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "row1" }]"#, Params::None);
				if let Some(e) = r.error {
					panic!("{e:?}");
				}
				let r = txn.rql(r#"INSERT nonexistent::table [{ id: 1 }]"#, Params::None);
				assert!(r.is_err());
				assert!(txn.commit().is_err(), "commit after poison must fail for {label}");
			}
			_ => unreachable!(),
		}

		assert_eq!(
			extract_rows(&t.query("FROM ns::t")),
			vec![],
			"poisoned transaction must not persist rows for {label}"
		);
	}
}

#[test]
fn test_drop_without_commit_rolls_back() {
	// Admin
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "alice" }]"#, Params::None);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		drop(txn);
		assert_eq!(extract_rows(&t.query("FROM ns::t")), vec![]);
	}

	// Command
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_command(IdentityId::system()).unwrap();
		let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "bob" }]"#, Params::None);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		drop(txn);
		assert_eq!(extract_rows(&t.query("FROM ns::t")), vec![]);
	}

	// Admin (Subscription removed)
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		let r = txn.rql(r#"INSERT ns::t [{ id: 1, name: "charlie" }]"#, Params::None);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		drop(txn);
		assert_eq!(extract_rows(&t.query("FROM ns::t")), vec![]);
	}
}

#[test]
fn test_rql_after_commit_errors() {
	// Admin
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		txn.commit().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Command
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_command(IdentityId::system()).unwrap();
		txn.commit().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Admin (Subscription removed)
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		txn.commit().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}
}

#[test]
fn test_rql_after_rollback_errors() {
	// Admin
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		txn.rollback().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Command
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_command(IdentityId::system()).unwrap();
		txn.rollback().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}

	// Admin (Subscription removed)
	{
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		txn.rollback().unwrap();
		assert!(txn.rql("FROM ns::t", Params::None).is_err());
	}
}

#[test]
fn test_rql_syntax_error_returns_err() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");

	// Admin
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);

	// Command
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);

	// Query
	let mut txn = t.begin_query(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);

	// Admin (Subscription removed)
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	assert!(txn.rql("INVALID GIBBERISH", Params::None).is_err());
	drop(txn);
}

#[test]
fn test_transaction_enum_rql() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::t { id: int8, name: utf8 }");
	t.command(r#"INSERT ns::t [{ id: 1, name: "alice" }]"#);

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let mut tx = Transaction::Admin(&mut txn);
	let r = tx.rql("FROM ns::t", Params::None);
	if let Some(e) = r.error {
		panic!("{e:?}");
	}
	assert_eq!(extract_rows(&r), vec![(1i64, "alice".to_string())]);
}
