// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// A deferred view created with a row TTL persists row settings in the same
// create-view commit; they must be findable via `find_row_settings` just like
// the transactional case.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn deferred_view_persists_row_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rs_d");
	t.admin("CREATE TABLE rs_d::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW rs_d::v { id: int4 } \
		 WITH { row: { ttl: { duration: '1h', mode: drop } } } \
		 AS { FROM rs_d::src MAP { id: id } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "rs_d").unwrap().unwrap();
	let view = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v")
		.unwrap()
		.expect("view must exist");
	let shape = view.underlying_id();

	let settings = catalog
		.find_row_settings(&mut Transaction::Admin(&mut txn), shape)
		.unwrap()
		.expect("row settings must be findable for the view's underlying shape");
	assert_eq!(settings.ttl.expect("row ttl must be set").duration_nanos, 3_600_000_000_000);
}
