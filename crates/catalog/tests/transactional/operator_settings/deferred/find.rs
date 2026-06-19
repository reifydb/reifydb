// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Deferred views write their operator TTL into the same create-view commit as
// transactional views; the settings must be findable via `find_operator_settings`
// just the same. The deferred registration path is the one that flaked in
// production, so this mirrors the transactional coverage for the deferred DDL.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn deferred_append_view_persists_operator_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE os_app_d");
	t.admin("CREATE TABLE os_app_d::s1 { id: int4, val: int4 }");
	t.admin("CREATE TABLE os_app_d::s2 { id: int4, val: int4 }");
	t.admin("CREATE DEFERRED VIEW os_app_d::merged { id: int4, val: int4 } AS { \
		 FROM os_app_d::s1 append { FROM os_app_d::s2 } with { ttl: { duration: \"500ms\" } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "os_app_d").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "merged")
		.unwrap()
		.expect("a flow must back the deferred view");
	let node_ids: Vec<_> = catalog
		.list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id)
		.unwrap()
		.into_iter()
		.map(|n| n.id)
		.collect();

	let mut ttls = Vec::new();
	for id in node_ids {
		if let Some(settings) = catalog.find_operator_settings(&mut Transaction::Admin(&mut txn), id).unwrap() {
			if let Some(ttl) = settings.ttl {
				ttls.push(ttl.duration_nanos);
			}
		}
	}

	assert_eq!(ttls, vec![500_000_000], "the append operator must carry its 500ms TTL");
}

#[test]
fn deferred_join_view_persists_join_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE os_join_d");
	t.admin("CREATE TABLE os_join_d::lhs { k: int4, lv: int4 }");
	t.admin("CREATE TABLE os_join_d::rhs { k: int4, rv: int4 }");
	t.admin("CREATE DEFERRED VIEW os_join_d::joined { k: int4, lv: int4, rv: int4 } AS { \
		 FROM os_join_d::lhs \
		 inner join { FROM os_join_d::rhs } as r using (k, r.k) with { ttl: { left: { duration: \"500ms\" } } } \
		 map { k: k, lv: lv, rv: r_rv } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "os_join_d").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "joined")
		.unwrap()
		.expect("a flow must back the deferred view");
	let node_ids: Vec<_> = catalog
		.list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id)
		.unwrap()
		.into_iter()
		.map(|n| n.id)
		.collect();

	let mut left_ttls = Vec::new();
	for id in node_ids {
		if let Some(settings) = catalog.find_operator_settings(&mut Transaction::Admin(&mut txn), id).unwrap() {
			if let Some(join) = settings.join {
				if let Some(left) = join.left {
					left_ttls.push(left.duration_nanos);
				}
			}
		}
	}

	assert_eq!(left_ttls, vec![500_000_000], "the join operator must carry its left-side 500ms TTL");
}
