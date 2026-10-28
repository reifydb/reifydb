// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::durability::Durability;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_a");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("DROP NAMESPACE ns_drop_a", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "ns_drop_a").unwrap();
	assert!(found.is_none(), "uncommitted DROP NAMESPACE must hide the namespace within its dropping txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		!all.iter().any(|n| n.name() == "ns_drop_a"),
		"uncommitted DROP NAMESPACE must remove the namespace from list_namespaces_all within its dropping txn"
	);
}

#[test]
fn rolled_back_drop_leaves_namespace_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_b");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("DROP NAMESPACE ns_drop_b", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_drop_b").unwrap();
	assert!(found.is_some(), "rolled-back DROP must leave the namespace visible in a later txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		all.iter().any(|n| n.name() == "ns_drop_b"),
		"rolled-back DROP must leave the namespace in list_namespaces_all in a later txn"
	);
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_c");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("DROP NAMESPACE ns_drop_c", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_drop_c").unwrap();
	assert!(found.is_none(), "committed DROP NAMESPACE must not be visible in a new txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		!all.iter().any(|n| n.name() == "ns_drop_c"),
		"committed DROP NAMESPACE must not appear in list_namespaces_all in a new txn"
	);
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_d");

	let mut txn1 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn1.rql("DROP NAMESPACE ns_drop_d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn2 = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_drop_d").unwrap();
	assert!(found_in_txn2.is_some(), "txn2 must still observe the namespace while txn1's DROP is uncommitted");
	let all_in_txn2 = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		all_in_txn2.iter().any(|n| n.name() == "ns_drop_d"),
		"txn2's list must still include the namespace while txn1's DROP is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn3 = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn3), "ns_drop_d").unwrap();
	assert!(found_in_txn3.is_none(), "after txn1 commits, the namespace must not be visible in a later txn");
}

#[test]
fn uncommitted_drop_hides_committed_children_within_txn() {
	// A namespace drop must hide every committed child it cascades, by name, by id and in lists.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_kids");
	t.admin("CREATE TABLE ns_drop_kids::t { id: int4 }");
	t.admin("CREATE DEFERRED VIEW ns_drop_kids::v { id: int4 } AS { FROM ns_drop_kids::t MAP { id: id } }");
	t.admin("CREATE DICTIONARY ns_drop_kids::d FOR utf8 AS uint2");
	t.admin("CREATE ENUM ns_drop_kids::status { Active, Inactive }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let ns_id = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "ns_drop_kids")
		.unwrap()
		.unwrap()
		.id();
	let table_id = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t").unwrap().unwrap().id;
	let view_id = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn), ns_id, "v").unwrap().unwrap().id();
	let r = txn.rql("DROP NAMESPACE ns_drop_kids", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(catalog.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t").unwrap().is_none());
	assert!(catalog.find_table(&mut Transaction::Admin(&mut txn), table_id).unwrap().is_none());
	assert!(!catalog.list_tables(&mut Transaction::Admin(&mut txn)).unwrap().iter().any(|x| x.id == table_id));

	assert!(catalog.find_view_by_name(&mut Transaction::Admin(&mut txn), ns_id, "v").unwrap().is_none());
	assert!(catalog.find_view(&mut Transaction::Admin(&mut txn), view_id).unwrap().is_none());
	assert!(!catalog.list_views_all(&mut Transaction::Admin(&mut txn)).unwrap().iter().any(|x| x.id() == view_id));

	assert!(catalog.find_dictionary_by_name(&mut Transaction::Admin(&mut txn), ns_id, "d").unwrap().is_none());
	assert!(catalog.list_dictionaries(&mut Transaction::Admin(&mut txn), ns_id).unwrap().is_empty());

	assert!(catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns_id, "status").unwrap().is_none());
	assert!(catalog.list_sumtypes(&mut Transaction::Admin(&mut txn), ns_id).unwrap().is_empty());
}
