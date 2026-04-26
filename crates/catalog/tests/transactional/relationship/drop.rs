// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::interface::catalog::relationship::RelationshipCardinality;
use reifydb_engine::test_prelude::*;
use reifydb_transaction::{change::TransactionalRelationshipChanges, transaction::Transaction};
use reifydb_type::fragment::Fragment;

use super::common::SourceFixture;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_drop_a");

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
		txn.commit().unwrap();
	}

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.drop_relationship(&mut txn, f.namespace, f.source_table, "owns").unwrap();

	// Same-txn lookup must miss.
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut txn), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_none(), "drop not visible within the same txn");

	// is_relationship_deleted_by_name must report true.
	assert!(TransactionalRelationshipChanges::is_relationship_deleted_by_name(
		&txn,
		f.namespace,
		f.source_table,
		"owns",
	));

	// list_relationships_from must omit it.
	let listed = catalog.list_relationships_from(&mut Transaction::Admin(&mut txn), f.source_table).unwrap();
	assert!(listed.is_empty(), "list still contains pending-deleted relationship");
}

#[test]
fn rolled_back_drop_leaves_relationship_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_drop_b");

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
		txn.commit().unwrap();
	}

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.drop_relationship(&mut txn, f.namespace, f.source_table, "owns").unwrap();
	txn.rollback().unwrap();

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut probe), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_some(), "rolled-back drop removed relationship");
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_drop_c");

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
		txn.commit().unwrap();
	}

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.drop_relationship(&mut txn, f.namespace, f.source_table, "owns").unwrap();
		txn.commit().unwrap();
	}

	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_relationship_by_name(&mut Transaction::Admin(&mut probe), f.namespace, f.source_table, "owns")
		.unwrap();
	assert!(found.is_none());
	assert!(catalog.materialized.find_relationship_by_name(f.namespace, f.source_table, "owns").is_none());
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
