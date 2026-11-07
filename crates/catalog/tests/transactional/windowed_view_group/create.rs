// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	bootstrap::load_catalog_cache, cache::CatalogCache, catalog::windowed_view_group::WindowedViewGroupToCreate,
};
use reifydb_core::interface::catalog::{
	flow::FlowId,
	id::{NamespaceId, ViewId},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::identity::IdentityId};

#[test]
fn uncommitted_create_is_visible_within_txn() {
	// The create path must see its own pending group, otherwise a second create in one txn would not clash.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let namespace = create_namespace(&t, "wvg_create_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let group = catalog.create_windowed_view_group(&mut txn, mk_group(namespace, "ohlcv")).unwrap();

	let by_name = catalog
		.find_windowed_view_group_by_name(&mut Transaction::Admin(&mut txn), namespace, "ohlcv")
		.unwrap();
	assert_eq!(by_name.as_ref().map(|g| g.id), Some(group.id));
	assert_eq!(by_name.map(|g| g.members), Some(members()));
}

#[test]
fn rolled_back_create_is_invisible_to_new_txn() {
	// A rolled back group must leave nothing behind in storage or cache.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let namespace = create_namespace(&t, "wvg_create_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_windowed_view_group(&mut txn, mk_group(namespace, "ohlcv")).unwrap();
	txn.rollback().unwrap();

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_windowed_view_group_by_name(&mut Transaction::Admin(&mut probe), namespace, "ohlcv")
		.unwrap();
	assert!(found.is_none(), "rolled-back group still visible: {:?}", found);
}

#[test]
fn committed_create_is_visible_via_materialized_in_new_txn() {
	// Without the post-commit interceptor arm the cache would miss the group and every read would hit storage.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let namespace = create_namespace(&t, "wvg_create_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let group = catalog.create_windowed_view_group(&mut txn, mk_group(namespace, "ohlcv")).unwrap();
	txn.commit().unwrap();

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_windowed_view_group_by_name(&mut Transaction::Admin(&mut probe), namespace, "ohlcv")
		.unwrap()
		.expect("group missing in new txn");
	assert_eq!(found.id, group.id);

	let cached = catalog
		.cache()
		.find_windowed_view_group_by_name(namespace, "ohlcv")
		.expect("group missing in catalog cache after commit");
	assert_eq!(cached.id, group.id);
	assert_eq!(cached.members, members());
}

#[test]
fn restart_recovery_repopulates_materialized_catalog() {
	// The loader must rebuild the group with its members in declaration order after a restart.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let namespace = create_namespace(&t, "wvg_create_d");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let group = catalog.create_windowed_view_group(&mut txn, mk_group(namespace, "ohlcv")).unwrap();
	txn.commit().unwrap();

	let fresh = CatalogCache::new();
	load_catalog_cache(t.inner().multi(), t.inner().single(), &fresh, &t.inner().services().runtime_context)
		.unwrap();

	let reloaded =
		fresh.find_windowed_view_group_by_name(namespace, "ohlcv").expect("group missing in rebuilt cache");
	assert_eq!(reloaded.id, group.id);
	assert_eq!(reloaded.flow, FlowId(5));
	assert_eq!(reloaded.members, members());
}

#[test]
fn dropped_group_is_gone_after_commit() {
	// A drop must clear both the cache entry and the by-name index.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let namespace = create_namespace(&t, "wvg_create_e");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let group = catalog.create_windowed_view_group(&mut txn, mk_group(namespace, "ohlcv")).unwrap();
	txn.commit().unwrap();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.drop_windowed_view_group(&mut txn, group).unwrap();
	txn.commit().unwrap();

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_windowed_view_group_by_name(&mut Transaction::Admin(&mut probe), namespace, "ohlcv")
		.unwrap();
	assert!(found.is_none(), "dropped group still visible: {:?}", found);
	assert!(catalog.cache().find_windowed_view_group_by_name(namespace, "ohlcv").is_none());
}

#[test]
fn drop_namespace_removes_its_groups() {
	// A group left behind by a namespace drop would resurrect under a recreated namespace with the same id.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let namespace = create_namespace(&t, "wvg_create_f");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_windowed_view_group(&mut txn, mk_group(namespace, "ohlcv")).unwrap();
	txn.commit().unwrap();

	t.admin("DROP NAMESPACE wvg_create_f");

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let listed = catalog.list_windowed_view_groups_all(&mut Transaction::Admin(&mut probe)).unwrap();
	assert!(listed.iter().all(|g| g.namespace != namespace), "group survived its namespace: {:?}", listed);
}

fn create_namespace(t: &TestEngine, name: &str) -> NamespaceId {
	t.admin(&format!("CREATE NAMESPACE {name}"));
	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	t.catalog().find_namespace_by_name(&mut Transaction::Admin(&mut probe), name).unwrap().unwrap().id()
}

fn members() -> Vec<ViewId> {
	vec![ViewId(30), ViewId(10), ViewId(20)]
}

fn mk_group(namespace: NamespaceId, name: &str) -> WindowedViewGroupToCreate {
	WindowedViewGroupToCreate {
		name: Fragment::internal(name),
		namespace,
		flow: FlowId(5),
		members: members(),
	}
}
