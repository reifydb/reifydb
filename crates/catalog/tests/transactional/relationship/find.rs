// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::interface::catalog::relationship::RelationshipCardinality;
use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::common::SourceFixture;

#[test]
fn admin_txn_find_short_circuits_on_pending_create() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_find_pending_create");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let rel = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();

	// Materialized cache is empty until commit.
	assert!(catalog.cache().find_relationship_by_name(f.namespace, f.source_table, "owns").is_none());

	// But the txn-local lookup sees the pending entry.
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut txn), f.namespace, f.source_table, "owns")
		.unwrap();
	assert_eq!(found.as_ref().map(|r| r.id), Some(rel.id));
}

#[test]
fn admin_txn_find_returns_none_after_pending_drop_even_if_materialized_has_it() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_find_pending_drop");

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
		txn.commit().unwrap();
	}

	// Cache has it.
	assert!(catalog.cache().find_relationship_by_name(f.namespace, f.source_table, "owns").is_some());

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.drop_relationship(&mut txn, f.namespace, f.source_table, "owns").unwrap();

	// In-txn lookup must observe the pending delete and return None even though
	// CatalogCache still holds the prior version.
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut txn), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_none());
}

#[test]
fn query_txn_does_not_see_uncommitted_create() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_find_query_isolation");

	let mut writer = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_relationship(&mut writer, mk_rel(&f, "owns")).unwrap();

	let mut reader = t.begin_query(IdentityId::system()).unwrap();
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Query(&mut reader), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_none());

	writer.commit().unwrap();
	drop(reader);

	let mut reader2 = t.begin_query(IdentityId::system()).unwrap();
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Query(&mut reader2), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_some());
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
