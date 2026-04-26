// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::interface::catalog::relationship::RelationshipCardinality;
use reifydb_engine::test_prelude::*;
use reifydb_type::fragment::Fragment;

use super::common::SourceFixture;

#[test]
fn duplicate_namespace_source_name_returns_already_exists() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_err_dup");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
	let err = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).expect_err("expected duplicate to fail");

	let msg = format!("{err:?}");
	assert!(msg.contains("CA_023") || msg.contains("already exists"), "wrong code: {msg}");
}

#[test]
fn many_to_many_without_junction_returns_mismatch() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_err_card");

	let to_create = RelationshipToCreate {
		name: Fragment::internal("owns"),
		namespace: f.namespace,
		source_table: f.source_table,
		source_column: f.source_column,
		target_table: f.target_table,
		target_column: f.target_column,
		junction: None,
		cardinality: RelationshipCardinality::ManyToMany,
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.create_relationship(&mut txn, to_create).expect_err("expected cardinality/junction mismatch");
	let msg = format!("{err:?}");
	assert!(msg.contains("CA_022") || msg.contains("THROUGH"), "wrong error: {msg}");
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
