// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_drop_a");
	t.admin("CREATE SERIES sens_drop_a::s { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP SERIES sens_drop_a::s", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(catalog.find_series_by_name(&mut Transaction::Admin(&mut txn), ns_id, "s").unwrap().is_none());
	let all = catalog.list_series_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "s"));
}

#[test]
fn rolled_back_drop_leaves_series_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_drop_b");
	t.admin("CREATE SERIES sens_drop_b::s { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP SERIES sens_drop_b::s", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_series_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap().is_some());
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_drop_c");
	t.admin("CREATE SERIES sens_drop_c::s { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP SERIES sens_drop_c::s", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_series_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap().is_none());
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_drop_d");
	t.admin("CREATE SERIES sens_drop_d::s { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP SERIES sens_drop_d::s", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_series_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap().is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_series_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "s").unwrap().is_none());
}
