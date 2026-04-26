// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::interface::catalog::{
	id::{ColumnId, NamespaceId, TableId},
	relationship::{RelationshipCardinality, RelationshipJunction},
};
use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::frame::{column::FrameColumn, frame::Frame},
};

struct Fixture {
	namespace: NamespaceId,
	source_table: TableId,
	source_column: ColumnId,
	target_table: TableId,
	target_column: ColumnId,
	junction_table: TableId,
	junction_source: ColumnId,
	junction_target: ColumnId,
}

impl Fixture {
	fn new(t: &TestEngine, ns: &str) -> Self {
		t.admin(&format!("CREATE NAMESPACE {ns}"));
		t.admin(&format!("CREATE TABLE {ns}::parent {{ id: int4 }}"));
		t.admin(&format!("CREATE TABLE {ns}::child {{ parent_id: int4 }}"));
		t.admin(&format!("CREATE TABLE {ns}::link {{ src_id: int4, tgt_id: int4 }}"));

		let catalog = t.catalog();
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let namespace =
			catalog.find_namespace_by_name(&mut Transaction::Admin(&mut probe), ns).unwrap().unwrap().id();
		let source_table = catalog
			.find_table_by_name(&mut Transaction::Admin(&mut probe), namespace, "parent")
			.unwrap()
			.unwrap()
			.id;
		let target_table = catalog
			.find_table_by_name(&mut Transaction::Admin(&mut probe), namespace, "child")
			.unwrap()
			.unwrap()
			.id;
		let junction_table = catalog
			.find_table_by_name(&mut Transaction::Admin(&mut probe), namespace, "link")
			.unwrap()
			.unwrap()
			.id;
		let source_columns = catalog.list_columns(&mut Transaction::Admin(&mut probe), source_table).unwrap();
		let source_column = source_columns.iter().find(|c| c.name == "id").unwrap().id;
		let target_columns = catalog.list_columns(&mut Transaction::Admin(&mut probe), target_table).unwrap();
		let target_column = target_columns.iter().find(|c| c.name == "parent_id").unwrap().id;
		let junction_columns =
			catalog.list_columns(&mut Transaction::Admin(&mut probe), junction_table).unwrap();
		let junction_source = junction_columns.iter().find(|c| c.name == "src_id").unwrap().id;
		let junction_target = junction_columns.iter().find(|c| c.name == "tgt_id").unwrap().id;

		Self {
			namespace,
			source_table,
			source_column,
			target_table,
			target_column,
			junction_table,
			junction_source,
			junction_target,
		}
	}
}

#[test]
fn vtable_returns_one_row_per_relationship_with_correct_columns() {
	let t = TestEngine::new();
	let f = Fixture::new(&t, "rel_vtable_a");
	let catalog = t.catalog();

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_simple(&f, "r_one_one", RelationshipCardinality::OneToOne))
			.unwrap();
		catalog.create_relationship(&mut txn, mk_simple(&f, "r_many_one", RelationshipCardinality::ManyToOne))
			.unwrap();
		catalog.create_relationship(&mut txn, mk_simple(&f, "r_one_many", RelationshipCardinality::OneToMany))
			.unwrap();
		catalog.create_relationship(
			&mut txn,
			RelationshipToCreate {
				name: Fragment::internal("r_many_many"),
				namespace: f.namespace,
				source_table: f.source_table,
				source_column: f.source_column,
				target_table: f.target_table,
				target_column: f.target_column,
				junction: Some(RelationshipJunction {
					table: f.junction_table,
					source_column: f.junction_source,
					target_column: f.junction_target,
				}),
				cardinality: RelationshipCardinality::ManyToMany,
			},
		)
		.unwrap();
		txn.commit().unwrap();
	}

	let frames = t.query("from system::relationships sort {id:ASC}");
	let frame = frames.first().expect("expected at least one frame");

	let expected = [
		"id",
		"namespace_id",
		"name",
		"source_table_id",
		"source_column_id",
		"target_table_id",
		"target_column_id",
		"junction_table_id",
		"junction_source_column_id",
		"junction_target_column_id",
		"cardinality",
	];
	for col_name in expected {
		assert!(frame.columns.iter().any(|c| c.name == col_name), "missing column {col_name}");
	}

	let cardinality = column(frame, "cardinality");
	assert_eq!(cardinality.data.len(), 4, "expected 4 relationship rows");

	let card_values: Vec<String> = (0..cardinality.data.len()).map(|i| cardinality.data.as_string(i)).collect();
	assert_eq!(card_values, vec!["1:1", "N:1", "1:N", "N:M"]);

	let names: Vec<String> = (0..4).map(|i| column(frame, "name").data.as_string(i)).collect();
	assert_eq!(names, vec!["r_one_one", "r_many_one", "r_one_many", "r_many_many"]);

	let junction_table_id = column(frame, "junction_table_id");
	assert_eq!(junction_table_id.data.as_string(0), "0");
	assert_eq!(junction_table_id.data.as_string(1), "0");
	assert_eq!(junction_table_id.data.as_string(2), "0");
	assert_eq!(junction_table_id.data.as_string(3), f.junction_table.0.to_string());

	let junction_source_column_id = column(frame, "junction_source_column_id");
	assert_eq!(junction_source_column_id.data.as_string(0), "0");
	assert_eq!(junction_source_column_id.data.as_string(3), f.junction_source.0.to_string());

	let junction_target_column_id = column(frame, "junction_target_column_id");
	assert_eq!(junction_target_column_id.data.as_string(0), "0");
	assert_eq!(junction_target_column_id.data.as_string(3), f.junction_target.0.to_string());

	let source_table_id = column(frame, "source_table_id");
	for i in 0..4 {
		assert_eq!(source_table_id.data.as_string(i), f.source_table.0.to_string());
	}

	let namespace_id = column(frame, "namespace_id");
	for i in 0..4 {
		assert_eq!(namespace_id.data.as_string(i), f.namespace.0.to_string());
	}
}

#[test]
fn vtable_filter_by_source_table_id_returns_only_matching_rows() {
	let t = TestEngine::new();
	let f = Fixture::new(&t, "rel_vtable_b");
	let catalog = t.catalog();

	{
		let mut txn = t.begin_admin(IdentityId::system()).unwrap();
		catalog.create_relationship(&mut txn, mk_simple(&f, "from_parent", RelationshipCardinality::OneToMany))
			.unwrap();
		catalog.create_relationship(
			&mut txn,
			RelationshipToCreate {
				name: Fragment::internal("from_child"),
				namespace: f.namespace,
				source_table: f.target_table,
				source_column: f.target_column,
				target_table: f.source_table,
				target_column: f.source_column,
				junction: None,
				cardinality: RelationshipCardinality::ManyToOne,
			},
		)
		.unwrap();
		txn.commit().unwrap();
	}

	let rql = format!(
		"from system::relationships filter {{source_table_id == {}}} sort {{name:ASC}}",
		f.source_table.0
	);
	let frames = t.query(&rql);
	let frame = frames.first().expect("expected at least one frame");

	let names: Vec<String> =
		(0..column(frame, "name").data.len()).map(|i| column(frame, "name").data.as_string(i)).collect();
	assert_eq!(names, vec!["from_parent"]);
}

fn column<'a>(frame: &'a Frame, name: &str) -> &'a FrameColumn {
	frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("missing column {name}"))
}

fn mk_simple(f: &Fixture, name: &str, cardinality: RelationshipCardinality) -> RelationshipToCreate {
	RelationshipToCreate {
		name: Fragment::internal(name),
		namespace: f.namespace,
		source_table: f.source_table,
		source_column: f.source_column,
		target_table: f.target_table,
		target_column: f.target_column,
		junction: None,
		cardinality,
	}
}
