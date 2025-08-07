// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::core::Frame;
use reifydb::core::interface::{Params, Principal};
use reifydb::engine::columnar::{Column, ColumnData, ColumnQualified, Columns};
use reifydb::engine::flow::change::{Diff, Change};
use reifydb::engine::flow::engine::FlowEngine;
use reifydb::engine::flow::flow::Flow;
use reifydb::engine::flow::node::NodeType;
use reifydb::session::{Session, SessionSync};
use reifydb::storage::memory::Memory;
use reifydb::transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb::transaction::svl::SingleVersionLock;
use reifydb::variant::embedded_sync::EmbeddedSync;
use reifydb::{ReifyDB, memory, optimistic, serializable};

fn main() {
    let mut db = ReifyDB::embedded_sync_with(optimistic(memory())).build();
    let session = db.command_session(Principal::root()).unwrap();

    session
        .command_sync(
            r#"
    create computed view test.adults { name: utf8, age: int1 }  with {
        from users
        filter { age > 18  and name == 'Bob' }
        map { name, age }
    }
    "#,
            Params::None,
        )
        .unwrap();

    rql_to_flow_example(&mut db);

}

fn rql_to_flow_example(
    db: &mut EmbeddedSync<Optimistic<Memory, SingleVersionLock<Memory>>, SingleVersionLock<Memory>>,
) {

    for frame in
        db.query_as_root("FROM reifydb.flows filter { id == 1 } map { id }", Params::None).unwrap()
    {
        println!("{}", frame);
    }

    let frame = db
        .query_as_root(
            "FROM reifydb.flows filter { id == 1 } map { cast(data, utf8) }",
            Params::None,
        )
        .unwrap()
        .pop()
        .unwrap();

    let value = frame[0].get_value(0);
    dbg!(&value.to_string());

    let flow: Flow = serde_json::from_str(value.to_string().as_str()).unwrap();

    // // Now let's execute the FlowGraph with real data
    println!("\n--- Executing FlowGraph with Sample Data ---");

    // Create engine and initialize
    let (versioned, unversioned, hooks) = memory();
    let mut engine = FlowEngine::new(
        flow.clone(),
        serializable((versioned.clone(), unversioned.clone(), hooks)).0,
        unversioned.clone(),
    );

    engine.initialize().unwrap();

    // Find the source node (users table)
    let source_node_id = flow
        .get_all_nodes()
        .find(|node_id| {
            if let Some(node) = flow.get_node(node_id) {
                matches!(node.ty, NodeType::Source { .. })
            } else {
                false
            }
        })
        .expect("Should have a source node");

    // Insert sample users with different ages
    let users_data =
        [("Alice", 16), ("Bob", 22), ("Charlie", 17), ("Diana", 25), ("Eve", 19), ("Bob", 60)];

    for (name, age) in users_data {
        println!("Inserting user: {} (age {})", name, age);

        // Create frame with user data
        // let frame = Frame::from_rows(
        //     &["name", "age"],
        //     &[vec![Value::Utf8(name.to_string()), Value::Int1(age)]],
        // );
        //

        let columns = Columns::new(vec![
            Column::ColumnQualified(ColumnQualified {
                name: "name".to_string(),
                data: ColumnData::utf8([name.to_string()]),
            }),
            Column::ColumnQualified(ColumnQualified {
                name: "age".to_string(),
                data: ColumnData::int1([age]),
            }),
        ]);

        // Process the change through the dataflow
        engine
            .process_change(
                &source_node_id,
                Change { diffs: vec![Diff::Insert { columns }], metadata: Default::default() },
            )
            .unwrap();
        //
    }

    // Query the computed view results
    println!("\n--- Computed View Results ---");
    let results = engine.get_view_data("adults").unwrap();
    let frame = Frame::from(results);
    println!("view contains {} rows:", frame.first().unwrap().data.len());
    println!("{}", frame);

}
