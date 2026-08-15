// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// A deferred view persists its row TTL in the same create-view commit, so the read contract
// must hold there too.

use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{duration::Duration, identity::IdentityId};

#[test]
fn deferred_view_persists_row_ttl() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rs_d");
	t.admin("CREATE TABLE rs_d::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW rs_d::v { id: int4 } \
		 WITH { row: { ttl: 1h, announce: false } } \
		 AS { FROM rs_d::src MAP { id: id } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "rs_d").unwrap().unwrap();
	let view = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v")
		.unwrap()
		.expect("view must exist");
	let shape = view.storage_id();

	let settings = catalog
		.find_row_settings(&mut Transaction::Admin(&mut txn), shape)
		.unwrap()
		.expect("row settings must be findable for the view's underlying shape");
	assert_eq!(settings.ttl.expect("row ttl must be set").duration, Duration::from_hours(1).unwrap());
}
