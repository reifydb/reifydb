// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::relationship::{Relationship, RelationshipCardinality},
};
use reifydb_engine::test_prelude::*;
use reifydb_type::fragment::Fragment;

use super::common::SourceFixture;

/// Drives CatalogCache::set_relationship directly to confirm:
/// - find_relationship_at returns the version-correct snapshot.
/// - The relationships_by_name index does NOT leak the prior name after a rename at v2 (catches the index-hygiene bug
///   fixed in this change).
#[test]
fn rename_does_not_leak_old_name_in_materialized_index() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_ver_rename");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let v1_rel = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
	let v_create = txn.commit().unwrap();

	// Drive a rename directly through the catalog cache (no rename API yet).
	let v2 = CommitVersion(v_create.0 + 1_000_000);
	let renamed = Relationship {
		name: "manages".to_string(),
		..v1_rel.clone()
	};
	catalog.cache().set_relationship(v1_rel.id, v2, Some(renamed.clone()));

	// Old name must no longer resolve at v2.
	assert!(catalog.cache().find_relationship_by_name_at(f.namespace, f.source_table, "owns", v2).is_none());

	// New name resolves at v2.
	let by_new = catalog.cache().find_relationship_by_name_at(f.namespace, f.source_table, "manages", v2);
	assert_eq!(by_new.map(|r| r.id), Some(v1_rel.id));
}

#[test]
fn historical_query_returns_old_version() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let f = SourceFixture::new(&t, "rel_ver_history");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let v1_rel = catalog.create_relationship(&mut txn, mk_rel(&f, "owns")).unwrap();
	let v_create = txn.commit().unwrap();

	// Pick a v_late far ahead of v_create.
	let v_late = CommitVersion(v_create.0 + 1_000_000);
	let renamed = Relationship {
		name: "manages".to_string(),
		..v1_rel.clone()
	};
	catalog.cache().set_relationship(v1_rel.id, v_late, Some(renamed));

	// At v_create we see the original name.
	let early_view = catalog.cache().find_relationship_at(v1_rel.id, v_create);
	let late_view = catalog.cache().find_relationship_at(v1_rel.id, v_late);

	assert_eq!(early_view.as_ref().map(|r| r.name.as_str()), Some("owns"));
	assert_eq!(late_view.as_ref().map(|r| r.name.as_str()), Some("manages"));
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
