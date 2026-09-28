// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

const SETUP_V_A: &str = "CREATE DEFERRED VIEW fns_list_a::v { id: int4 } AS { FROM fns_list_a::src MAP { id: id } }";
const SETUP_V_B: &str = "CREATE DEFERRED VIEW fns_list_b::v { id: int4 } AS { FROM fns_list_b::src MAP { id: id } }";
const SETUP_V_C: &str = "CREATE DEFERRED VIEW fns_list_c::v { id: int4 } AS { FROM fns_list_c::src MAP { id: id } }";
const SETUP_V_D: &str = "CREATE DEFERRED VIEW fns_list_d::v { id: int4 } AS { FROM fns_list_d::src MAP { id: id } }";
const SETUP_V_E: &str = "CREATE DEFERRED VIEW fns_list_e::v { id: int4 } AS { FROM fns_list_e::src MAP { id: id } }";

#[test]
fn committed_flow_is_listed() {
	// Drop DDLs and flow-sync find a view's flow only through this list; a committed flow missing here is never
	// dropped or run.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_list_a");
	t.admin("CREATE TABLE fns_list_a::src { id: int4 }");
	t.admin(SETUP_V_A);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "fns_list_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut q = t.begin_query(IdentityId::system()).unwrap();
	let all = catalog.list_flows_all(&mut Transaction::Query(&mut q)).unwrap();
	assert!(
		all.iter().any(|f| f.namespace == ns_id && f.name == "v"),
		"committed flow must appear in list_flows_all"
	);
}

#[test]
fn committed_drop_removes_flow_from_list() {
	// Otherwise a dropped flow is loaded and run again against a view that no longer exists.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_list_b");
	t.admin("CREATE TABLE fns_list_b::src { id: int4 }");
	t.admin(SETUP_V_B);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "fns_list_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	t.admin("DROP VIEW fns_list_b::v");

	let mut q = t.begin_query(IdentityId::system()).unwrap();
	let all = catalog.list_flows_all(&mut Transaction::Query(&mut q)).unwrap();
	assert!(
		!all.iter().any(|f| f.namespace == ns_id && f.name == "v"),
		"flow dropped by a committed DROP VIEW must not appear in list_flows_all"
	);
}

#[test]
fn uncommitted_create_is_listed_within_txn() {
	// Without the pending-change merge a cache read misses the txn's own new flow, so a later DDL in the same txn
	// cannot see it.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_list_c");
	t.admin("CREATE TABLE fns_list_c::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "fns_list_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(SETUP_V_C, Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let all = catalog.list_flows_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		all.iter().any(|f| f.namespace == ns_id && f.name == "v"),
		"uncommitted flow must appear in list_flows_all within its creating txn"
	);
}

#[test]
fn uncommitted_drop_is_not_listed_within_txn() {
	// Without the pending-drop filter the cache still returns the committed flow, so the txn would act on a flow it
	// already dropped.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_list_d");
	t.admin("CREATE TABLE fns_list_d::src { id: int4 }");
	t.admin(SETUP_V_D);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "fns_list_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP VIEW fns_list_d::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let all = catalog.list_flows_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		!all.iter().any(|f| f.namespace == ns_id && f.name == "v"),
		"flow dropped within the txn must not appear in list_flows_all before commit"
	);
}

#[test]
fn dropped_flow_is_still_listed_at_version_before_drop() {
	// A reader must see the flows of its own snapshot; reading the latest cache entry instead would hide a flow it
	// can still observe.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_list_e");
	t.admin("CREATE TABLE fns_list_e::src { id: int4 }");
	t.admin(SETUP_V_E);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "fns_list_e")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut before_drop = t.begin_query(IdentityId::system()).unwrap();

	t.admin("DROP VIEW fns_list_e::v");

	let old = catalog.list_flows_all(&mut Transaction::Query(&mut before_drop)).unwrap();
	assert!(
		old.iter().any(|f| f.namespace == ns_id && f.name == "v"),
		"reader at a version before the drop must still list the flow"
	);

	let mut after_drop = t.begin_query(IdentityId::system()).unwrap();
	let new = catalog.list_flows_all(&mut Transaction::Query(&mut after_drop)).unwrap();
	assert!(
		!new.iter().any(|f| f.namespace == ns_id && f.name == "v"),
		"reader at a version after the drop must not list the flow"
	);
}
