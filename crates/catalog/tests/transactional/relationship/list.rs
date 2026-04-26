// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::interface::catalog::relationship::RelationshipCardinality;
use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::common::SourceFixture;

#[test]
fn list_from_includes_pending_create_and_excludes_pending_drop() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_list_a");

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
		txn.commit().unwrap();
	}

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();

	catalog.create_relationship(&mut txn, mk_rel(&f, "manages")).unwrap();
	catalog.drop_relationship(&mut txn, f.namespace, f.source_table, "owns").unwrap();

	let mut listed = catalog.list_relationships_from(&mut Transaction::Admin(&mut txn), f.source_table).unwrap();
	listed.sort_by(|a, b| a.name.cmp(&b.name));

	assert_eq!(listed.len(), 1);
	assert_eq!(listed[0].name, "manages");
}

#[test]
fn list_from_uses_materialized_cache_in_command_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_list_b");

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
		catalog.create_relationship(&mut txn, mk_rel(&f, "manages")).unwrap();
		txn.commit().unwrap();
	}

	let mut q = t.begin_query(IdentityId::system()).unwrap();
	let listed = catalog.list_relationships_from(&mut Transaction::Query(&mut q), f.source_table).unwrap();
	assert_eq!(listed.len(), 2);
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
