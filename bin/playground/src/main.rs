// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::collections::Bound::Included;

use reifydb::{
	MemoryDatabaseOptimistic, SessionSync,
	core::{
		EncodedKeyRange, Frame, Type,
		interface::{
			ColumnId, ColumnIndex, EncodableKeyRange, Params,
			SchemaId, Table, TableId, TableRowKeyRange,
		},
		row::EncodedRowLayout,
	},
	engine::{
		columnar::Columns,
		flow::{
			flow::Flow,
			node::{NodeId, NodeType},
		},
	},
	sync,
};

pub type DB = MemoryDatabaseOptimistic;
// pub type DB = SqliteDatabaseOptimistic;

fn main() {
	let mut db: DB = sync::memory_optimistic();
	// let mut db: DB =
	// sync::sqlite_optimistic(SqliteConfig::new("/tmp/reifydb"));

	db.start().unwrap();

	db.command_as_root(
		r#"
	    create schema test;
	    create table test.users { name: utf8, age: int1 };
	"#,
		Params::None,
	)
	.unwrap();

	// Skip computed view for now since flow subsystem has unimplemented
	db.command_as_root(
		r#"
	create computed view test.adults { name: utf8, age: int1 }  with {
	    from test.users
	    filter { age > 18  }
	    map { name, age }
	}
	"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
    from [
        { name: "bob", age: 16 },
        { name: "lucy", age: 20 },
        { name: "juciy", age: 19 },
    ]
    insert test.users;

    "#,
		Params::None,
	)
	.unwrap();

	for frame in
		db.query_as_root(r#"FROM test.users"#, Params::None).unwrap()
	{
		println!("{}", frame);
	}

	db.command_as_root(
		r#"
    from test.users
    filter { name = "bob" }
    map { name: "bob", age: 21}
    update test.users;

    "#,
		Params::None,
	)
	.unwrap();

	for frame in
		db.query_as_root(r#"FROM test.users"#, Params::None).unwrap()
	{
		println!("{}", frame);
	}

	loop {}

	// println!("Basic database operations completed successfully!");
	// rql_to_flow_example(&mut db);
}

fn _rql_to_flow_example(db: &mut DB) {
	// for frame in
	//     db.query_as_root("FROM reifydb.flows filter { id == 1 } map { id
	// }", Params::None).unwrap() {
	//     println!("{}", frame);
	// }
	//
	let frame = db
		.query_as_root(
			"FROM reifydb.flows filter { id == 1 } map { cast(data, utf8) }",
			Params::None,
		)
		.unwrap()
		.pop()
		.unwrap();

	let value = frame[0].get_value(0);
	// dbg!(&value.to_string());

	let flow: Flow =
		serde_json::from_str(value.to_string().as_str()).unwrap();

	// // // Now let's execute the FlowGraph with real data
	// println!("\n--- Executing FlowGraph with Sample Data ---");
	//
	// // Create engine and initialize
	// let (versioned, unversioned, hooks) = memory();
	// let mut processor = FlowProcessor::new(
	//     flow.clone(),
	//     serializable((versioned.clone(), unversioned.clone(), hooks)).0,
	//     unversioned.clone(),
	// );
	//
	// processor.initialize().unwrap();
	//
	// // Find the source node (users table)
	// let source_node_id = flow
	//     .get_all_nodes()
	//     .find(|node_id| {
	//         if let Some(node) = flow.get_node(node_id) {
	//             matches!(node.ty, NodeType::Source { .. })
	//         } else {
	//             false
	//         }
	//     })
	//     .expect("Should have a source node");
	//
	// // Insert sample users with different ages
	// let users_data =
	//     [("Alice", 16), ("Bob", 22), ("Charlie", 17), ("Diana", 25),
	// ("Eve", 19), ("Bob", 60)];
	//
	// for (name, age) in users_data {
	//     println!("Inserting user: {} (age {})", name, age);
	//
	//     // Create frame with user data
	//     // let frame = Frame::from_rows(
	//     //     &["name", "age"],
	//     //     &[vec![Value::Utf8(name.to_string()), Value::Int1(age)]],
	//     // );
	//     //
	//
	//     let columns = Columns::new(vec![
	//         Column::ColumnQualified(ColumnQualified {
	//             name: "name".to_string(),
	//             data: ColumnData::utf8([name.to_string()]),
	//         }),
	//         Column::ColumnQualified(ColumnQualified {
	//             name: "age".to_string(),
	//             data: ColumnData::int1([age]),
	//         }),
	//     ]);
	//
	//     dbg!(&source_node_id);
	//
	//     // Process the change through the dataflow
	//     processor
	//         .process_change(
	//             &source_node_id,
	//             Change { diffs: vec![Diff::Insert { columns }], metadata:
	// Default::default() },         )
	//         .unwrap();
	//     //
	// }

	// Query the computed view results
	println!("\n--- Computed View Results ---");
	let results = get_view_data(db, &flow, "adults").unwrap();
	// let results = reifydb
	//     .query_as_root(
	//         r#"
	//     from test.users
	// "#,
	//         Params::None,
	//     )
	//     .unwrap()
	//     .pop()
	//     .unwrap();
	let frame = Frame::from(results);
	println!("view contains {} rows:", frame.first().unwrap().data.len());
	println!("{}", frame);
}

pub fn get_view_data(
	db: &mut DB,
	flow: &Flow,
	view_name: &str,
) -> reifydb::Result<Columns> {
	// Find view node and read from versioned storage
	for node_id in flow.get_all_nodes() {
		if let Some(node) = flow.get_node(&node_id) {
			if let NodeType::Sink {
				name,
				..
			} = &node.ty
			{
				dbg!(&name);
				if name == view_name {
					dbg!(&node_id);
					return read_columns_from_storage(
						db, &node_id,
					);
				}
			}
		}
	}
	panic!("View {} not found", view_name);
}

fn read_columns_from_storage(
	db: &mut DB,
	node_id: &NodeId,
) -> reifydb::Result<Columns> {
	let range = TableRowKeyRange {
		table: TableId(node_id.0),
	};
	let versioned_data = db
		.engine()
		.versioned()
		.range(
			EncodedKeyRange::new(
				Included(range.start().unwrap()),
				Included(range.end().unwrap()),
			),
			10,
		)
		.unwrap();

	let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Int1]);

	let table = Table {
		id: TableId(node_id.0),
		schema: SchemaId(0),
		name: "view".to_string(),
		columns: vec![
			reifydb::core::interface::Column {
				id: ColumnId(0),
				name: "name".to_string(),
				ty: Type::Utf8,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
			reifydb::core::interface::Column {
				id: ColumnId(1),
				name: "age".to_string(),
				ty: Type::Int1,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
			},
		],
	};

	let mut columns = Columns::empty_from_table(&table);
	let mut iter = versioned_data.into_iter();
	while let Some(versioned) = iter.next() {
		columns.append_rows(&layout, [versioned.row])?;
	}
	Ok(columns)
}
