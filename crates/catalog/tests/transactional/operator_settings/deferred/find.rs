// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Deferred views write their operator TTL into the same create-view commit as transactional
// views, but register through a separate path, so the read contract is covered twice.

use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{duration::Duration, identity::IdentityId};

#[test]
fn deferred_append_view_persists_no_operator_settings() {
	// Append is stateless, so it must reach the catalog carrying nothing a reaper could act on.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE os_app_d");
	t.admin("CREATE TABLE os_app_d::s1 { id: int4, val: int4 }");
	t.admin("CREATE TABLE os_app_d::s2 { id: int4, val: int4 }");
	t.admin("CREATE DEFERRED VIEW os_app_d::merged { id: int4, val: int4 } AS { \
		 FROM os_app_d::s1 append { FROM os_app_d::s2 } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "os_app_d").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "merged")
		.unwrap()
		.expect("a flow must back the deferred view");
	let node_ids: Vec<_> = catalog
		.list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id)
		.unwrap()
		.into_iter()
		.map(|n| n.id)
		.collect();

	let mut ttls = Vec::new();
	for id in node_ids {
		if let Some(settings) = catalog.find_operator_settings(&mut Transaction::Admin(&mut txn), id).unwrap() {
			if let Some(ttl) = settings.retention {
				ttls.push(ttl.duration);
			}
		}
	}

	assert!(ttls.is_empty(), "no operator in an append flow may carry a retention, found {ttls:?}");
}

#[test]
fn deferred_append_view_rejects_a_retention() {
	// Without a parse-time rejection the clause is accepted and dropped, leaving the user believing state is
	// bounded.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE os_app_r");
	t.admin("CREATE TABLE os_app_r::s1 { id: int4, val: int4 }");
	t.admin("CREATE TABLE os_app_r::s2 { id: int4, val: int4 }");

	t.admin_err(
		"CREATE DEFERRED VIEW os_app_r::merged { id: int4, val: int4 } AS { \
		 FROM os_app_r::s1 append { FROM os_app_r::s2 } with { retention: 1s } }",
	);
}

#[test]
fn deferred_join_view_persists_join_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE os_join_d");
	t.admin("CREATE TABLE os_join_d::lhs { k: int4, lv: int4, at: datetime } with { time: event(at) }");
	t.admin("CREATE TABLE os_join_d::rhs { k: int4, rv: int4, at: datetime } with { time: event(at) }");
	t.admin("CREATE DEFERRED VIEW os_join_d::joined { k: int4, lv: int4, rv: int4 } AS { \
		 FROM os_join_d::lhs \
		 inner join { FROM os_join_d::rhs } as r using (k, r.k) with { retention: { left: 1s } } \
		 map { k: k, lv: lv, rv: r_rv } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "os_join_d").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "joined")
		.unwrap()
		.expect("a flow must back the deferred view");
	let node_ids: Vec<_> = catalog
		.list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id)
		.unwrap()
		.into_iter()
		.map(|n| n.id)
		.collect();

	let mut left_ttls = Vec::new();
	for id in node_ids {
		if let Some(settings) = catalog.find_operator_settings(&mut Transaction::Admin(&mut txn), id).unwrap() {
			if let Some(join) = settings.join {
				if let Some(left) = join.left {
					left_ttls.push(left.duration);
				}
			}
		}
	}

	assert_eq!(
		left_ttls,
		vec![Duration::from_seconds(1).unwrap()],
		"the join operator must carry its left-side 1s TTL"
	);
}
