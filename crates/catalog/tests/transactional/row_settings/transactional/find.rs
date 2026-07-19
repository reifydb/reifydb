// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// A transactional view created with a row TTL must persist row settings against
// its underlying shape, findable via `find_row_settings` at the transaction's
// version. Same read-path contract as operator settings.

use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{duration::Duration, identity::IdentityId};

#[test]
fn transactional_view_persists_row_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rs_t");
	t.admin("CREATE TABLE rs_t::src { id: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW rs_t::v { id: int4 } \
		 WITH { row: { ttl: { duration: '1h', mode: drop } } } \
		 AS { FROM rs_t::src MAP { id: id } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "rs_t").unwrap().unwrap();
	let view = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v")
		.unwrap()
		.expect("view must exist");
	let shape = view.underlying_id();

	let settings = catalog
		.find_row_settings(&mut Transaction::Admin(&mut txn), shape)
		.unwrap()
		.expect("row settings must be findable for the view's underlying shape");
	assert_eq!(settings.ttl.expect("row ttl must be set").duration, Duration::from_hours(1).unwrap());
}
