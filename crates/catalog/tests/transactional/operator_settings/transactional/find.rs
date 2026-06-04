// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB
//
// A transactional view whose flow carries an operator TTL (append / join) must
// persist that TTL as operator settings, findable via `find_operator_settings`
// at the registering transaction's version. This is the read path flow
// registration relies on to build TTL-aware operators; a miss here silently
// disables eviction.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn transactional_append_view_persists_operator_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE os_app_t");
	t.admin("CREATE TABLE os_app_t::s1 { id: int4, val: int4 }");
	t.admin("CREATE TABLE os_app_t::s2 { id: int4, val: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW os_app_t::merged { id: int4, val: int4 } AS { \
		 FROM os_app_t::s1 append { FROM os_app_t::s2 } with { ttl: { duration: \"500ms\" } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "os_app_t").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "merged")
		.unwrap()
		.expect("a flow must back the transactional view");
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
fn transactional_join_view_persists_join_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE os_join_t");
	t.admin("CREATE TABLE os_join_t::lhs { k: int4, lv: int4 }");
	t.admin("CREATE TABLE os_join_t::rhs { k: int4, rv: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW os_join_t::joined { k: int4, lv: int4, rv: int4 } AS { \
		 FROM os_join_t::lhs \
		 inner join { FROM os_join_t::rhs } as r using (k, r.k) with { ttl: { left: { duration: \"500ms\" } } } \
		 map { k: k, lv: lv, rv: r_rv } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "os_join_t").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "joined")
		.unwrap()
		.expect("a flow must back the transactional view");
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
