// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_list_a");
	t.admin("CREATE SERIES sens_list_a::keep { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_list_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE SERIES sens_list_a::new { ts: datetime, val: float8 } WITH { key: ts }", Params::None);
	txn.rql("DROP SERIES sens_list_a::keep", Params::None);

	let all = catalog.list_series_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "new"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_list_b");
	t.admin("CREATE SERIES sens_list_b::keep { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_list_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE SERIES sens_list_b::new { ts: datetime, val: float8 } WITH { key: ts }", Params::None);
	txn.rql("DROP SERIES sens_list_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_series_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "new"));
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_list_c");
	t.admin("CREATE SERIES sens_list_c::keep { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_list_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE SERIES sens_list_c::new { ts: datetime, val: float8 } WITH { key: ts }", Params::None);
	txn.rql("DROP SERIES sens_list_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_series_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "new"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE sens_list_d");
	t.admin("CREATE SERIES sens_list_d::keep { ts: datetime, val: float8 } WITH { key: ts }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "sens_list_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE SERIES sens_list_d::new { ts: datetime, val: float8 } WITH { key: ts }", Params::None);
	txn1.rql("DROP SERIES sens_list_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 = catalog.list_series_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!in_txn2.iter().any(|x| x.namespace == ns_id && x.name() == "new"));
	assert!(in_txn2.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 = catalog.list_series_all(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(in_txn3.iter().any(|x| x.namespace == ns_id && x.name() == "new"));
	assert!(!in_txn3.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));
}
