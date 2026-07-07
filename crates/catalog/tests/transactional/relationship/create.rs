// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	bootstrap::load_catalog_cache, catalog::relationship::RelationshipToCreate,
	cache::CatalogCache,
};
use reifydb_core::interface::catalog::relationship::RelationshipCardinality;
use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use super::common::SourceFixture;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_create_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let rel = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).expect("create_relationship failed");

	// Visible by id within the same txn
	let by_id = catalog.find_relationship(&mut Transaction::Admin(&mut txn), rel.id).unwrap();
	assert_eq!(by_id.as_ref().map(|r| r.id), Some(rel.id));

	// Visible by name within the same txn
	let by_name = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut txn), f.namespace, f.source_table, "owns")
		.unwrap();
	assert_eq!(by_name.as_ref().map(|r| r.id), Some(rel.id));

	// list_relationships_from sees the pending create
	let listed = catalog.list_relationships_from(&mut Transaction::Admin(&mut txn), f.source_table).unwrap();
	assert_eq!(listed.len(), 1);
	assert_eq!(listed[0].id, rel.id);
}

#[test]
fn rolled_back_create_is_invisible_to_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_create_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
	txn.rollback().unwrap();

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut probe), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_none(), "rolled-back relationship still visible: {:?}", found);
}

#[test]
fn committed_create_is_visible_via_materialized_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_create_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let rel = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
	txn.commit().unwrap();

	// New txn -> the post-commit interceptor must have populated CatalogCache.
	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut probe), f.namespace, f.source_table, "owns")
		.unwrap()
		.expect("relationship missing in new txn");
	assert_eq!(found.id, rel.id);

	// Materialized cache holds the entry directly (not via storage fallback).
	let mat = catalog
		.cache()
		.find_relationship_by_name(f.namespace, f.source_table, "owns")
		.expect("relationship missing in catalog cache after commit");
	assert_eq!(mat.id, rel.id);
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_create_d");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_relationship(&mut txn1, mk_rel(&f, "owns")).unwrap();

	// Concurrent admin txn must NOT see the uncommitted relationship.
	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut txn2), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut txn3), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(in_txn3.is_some());
}

#[test]
fn restart_recovery_repopulates_materialized_catalog() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_create_e");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let rel = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
	txn.commit().unwrap();

	// Simulate a restart: fresh CatalogCache, run loader against the same stores.
	let fresh = CatalogCache::new();
	load_catalog_cache(t.inner().multi(), t.inner().single(), &fresh).unwrap();

	let by_name = fresh
		.find_relationship_by_name(f.namespace, f.source_table, "owns")
		.expect("relationship missing in rebuilt CatalogCache");
	assert_eq!(by_name.id, rel.id);
}

fn mk_rel(f: &SourceFixture, name: &str) -> RelationshipToCreate {
	RelationshipToCreate {
		name: Fragment::internal(name),
		namespace: f.namespace,
		source_table: f.source_table,
		source_column: f.source_column,
		target_table: f.target_table,
		target_column: f.target_column,
		junction: None,
		cardinality: RelationshipCardinality::OneToMany,
	}
}
