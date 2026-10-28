// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::durability::Durability;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_a");
	t.admin("CREATE TABLE vns_create_a::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_a::v { id: int4 } AS { FROM vns_create_a::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn), ns_id, "v").unwrap();
	assert!(found.is_some());

	let all = catalog.list_views_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace() == ns_id && x.name() == "v"));
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_b");
	t.admin("CREATE TABLE vns_create_b::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_b::v { id: int4 } AS { FROM vns_create_b::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v").unwrap();
	assert!(found.is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_c");
	t.admin("CREATE TABLE vns_create_c::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_c::v { id: int4 } AS { FROM vns_create_c::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v").unwrap();
	assert!(found.is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_d");
	t.admin("CREATE TABLE vns_create_d::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn1.rql(
		"CREATE DEFERRED VIEW vns_create_d::v { id: int4 } AS { FROM vns_create_d::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn2 = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v").unwrap();
	assert!(found_in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn3 = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "v").unwrap();
	assert!(found_in_txn3.is_some());
}

#[test]
fn create_with_non_query_body_is_rejected_with_query_005() {
	// Views must keep QUERY_005 for a non-query body, otherwise the subscription-only SUBS_010 leaked into views.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE vns_create_e");
	t.admin("CREATE TABLE vns_create_e::src { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_e::v { id: int4 } AS { CREATE TABLE vns_create_e::other { id: int4 } }",
		Params::None,
	);
	let diagnostic = r.error.expect("a deferred view whose AS body is DDL must be rejected").diagnostic();
	assert_eq!(diagnostic.code, "QUERY_005", "got: {}", diagnostic.message);
}

#[test]
fn create_drop_create_same_name_in_one_txn_succeeds() {
	// A drop must hide an earlier in-txn create, otherwise the recreate fails with CA_003.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_f");
	t.admin("CREATE TABLE vns_create_f::src { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_f::v { id: int4 } AS { FROM vns_create_f::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "first create failed: {:?}", r.error);
	let r = txn.rql("DROP VIEW vns_create_f::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_f::v { id: int4 } AS { FROM vns_create_f::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "recreate after in-txn drop failed: {:?}", r.error);

	let ns_id = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "vns_create_f")
		.unwrap()
		.unwrap()
		.id();
	let views: Vec<_> = catalog
		.list_views_all(&mut Transaction::Admin(&mut txn))
		.unwrap()
		.into_iter()
		.filter(|x| x.namespace() == ns_id && x.name() == "v")
		.collect();
	assert_eq!(views.len(), 1, "exactly one live view may carry the name after recreate");
	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn), ns_id, "v").unwrap().unwrap();
	assert_eq!(found.id(), views[0].id(), "lookup by name must return the recreated view, never the dropped one");
}

#[test]
fn migrate_applies_create_then_drop_and_recreate_across_migrations() {
	// MIGRATE applies all pending migrations in one txn, so a recreate must not see the earlier create.
	let t = TestEngine::new();
	t.admin(
		"CREATE MIGRATION '001_create' { CREATE NAMESPACE vns_create_g; CREATE TABLE vns_create_g::src { id: int4 }; CREATE DEFERRED VIEW vns_create_g::v { id: int4 } AS { FROM vns_create_g::src MAP { id: id } }; }",
	);
	t.admin(
		"CREATE MIGRATION '002_recreate' { DROP VIEW vns_create_g::v; CREATE DEFERRED VIEW vns_create_g::v { id: int4 } AS { FROM vns_create_g::src MAP { id: id } }; }",
	);
	t.admin("MIGRATE");

	let frames = t.query("FROM system::views FILTER {name == \"v\"}");
	assert_eq!(TestEngine::row_count(&frames), 1, "exactly one view named v must survive the migrations");
}

#[test]
fn single_migration_create_drop_create_succeeds() {
	// One migration body runs in one txn, so create, drop and recreate of a name must not raise CA_003.
	let t = TestEngine::new();
	t.admin(
		"CREATE MIGRATION '001_all' { CREATE NAMESPACE vns_create_h; CREATE TABLE vns_create_h::src { id: int4 }; CREATE DEFERRED VIEW vns_create_h::v { id: int4 } AS { FROM vns_create_h::src MAP { id: id } }; DROP VIEW vns_create_h::v; CREATE DEFERRED VIEW vns_create_h::v { id: int4 } AS { FROM vns_create_h::src MAP { id: id } }; }",
	);
	t.admin("MIGRATE");

	let frames = t.query("FROM system::views FILTER {name == \"v\"}");
	assert_eq!(TestEngine::row_count(&frames), 1, "exactly one view named v must survive the migration");
}
