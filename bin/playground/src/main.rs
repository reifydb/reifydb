// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::core::Frame;
use reifydb::core::interface::{Params, Principal};
use reifydb::engine::columnar::{Column, ColumnData, ColumnQualified, Columns};
use reifydb::engine::flow::change::{Change, Diff};
use reifydb::engine::flow::compile::compile_to_flow;
use reifydb::engine::flow::engine::FlowEngine;
use reifydb::engine::flow::flow::Flow;
use reifydb::engine::flow::node::NodeType;
use reifydb::rql::ast;
use reifydb::rql::plan::logical::compile_logical;
use reifydb::session::{Session, SessionSync};
use reifydb::{ReifyDB, memory, optimistic, serializable};

fn main() {
    let db = ReifyDB::embedded_sync_with(optimistic(memory())).build();
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

    // db.command_as_root(r#"create schema test"#).unwrap();
    // let err = db.command_as_root(r#"create table test.arith { id: int2, from: int2, num: int2 }"#).unwrap_err();
    // dbg!(&err);
    // println!("{}", err);

    //     db.command_as_root(r#"create table test.one(field: int1, other: int1)"#).unwrap();
    //     db.command_as_root(r#"create table test.two(field: int1, name: text)"#).unwrap();
    //     db.command_as_root(r#"create table test.three(field: int1, type: text)"#).unwrap();
    //     db.command_as_root(r#"from [{field: 1, other: 2}, {field: 2, other: 2}, {field: 3, other: 2}, {field: 4, other: 2}, {field: 5, other: 2}] insert test.one"#).unwrap();
    //     db.command_as_root(
    //         r#"from [{field: 2, name: "Peter"}, {field: 5, name: "Parker"}] insert test.two"#,
    //     )
    //     .unwrap();
    //     db.command_as_root(r#"from [{field: 5, type: "Barker"}] insert test.three"#).unwrap();
    //
    //     for frame in db
    //         .command_as_root(
    //             r#"
    // map {
    //   cast(1.0, float8) + cast(1.0, float8),
    //   cast(1.0, float8) + cast(-1.0, float8),
    //   cast(-1.0, float8) + cast(-1.0, float8),
    //   cast(1.1, float8) + cast(1.1, float8),
    // }
    //         "#,
    //         )
    //         .unwrap()
    //     {
    //         println!("{}", frame);
    //     }

    // Test RQL to FlowGraph compilation
    rql_to_flow_example();

    // Dataflow example
    // dataflow_example();
}

fn rql_to_flow_example() {
    println!("\n=== RQL to FlowGraph Compilation Example ===");

    // Parse a simple RQL query
//     let rql = r#"
// create computed view test.adults { name: utf8, age: int1 }  with {
//     from users
//     filter { age > 18  and name == 'Bob' }
//     map { name, age }
// }
// "#;

    // println!("Compiling RQL: {}", rql);
    //
    // // Parse RQL into AST
    // let ast_statements = match ast::parse(rql) {
    //     Ok(statements) => statements,
    //     Err(e) => {
    //         println!("RQL parsing failed: {}", e);
    //         return;
    //     }
    // };
    //
    // println!("AST statements: {} nodes", ast_statements.len());
    //
    // // Compile AST to logical plans
    // let logical_plans = match compile_logical(ast_statements.into_iter().next().unwrap()) {
    //     Ok(plans) => plans,
    //     Err(e) => {
    //         println!("Logical plan compilation failed: {}", e);
    //         return;
    //     }
    // };

    // println!("Logical plans: {} nodes", logical_plans.len());
    // for (i, plan) in logical_plans.iter().enumerate() {
    //     println!("  Plan {}: {:?}", i, plan);
    // }

    let db =
        // ReifyDB::embedded_sync_with(optimistic(sqlite(SqliteConfig::new("/tmp/flow")))).build();
        ReifyDB::embedded_sync_with(optimistic(memory())).build();

    // Compile logical plans to FlowGraph
    // match compile_to_flow(logical_plans) {
    //     Ok(flow) => {
    //         println!("✅ Successfully compiled Flow!");
    //         println!("Flow has {} nodes", flow.get_all_nodes().count());
    //
    //         // Print the nodes in the graph
    //         // for node_id in flow.get_all_nodes() {
    //         //     if let Some(node) = flow.get_node(&node_id) {
    //         //         // println!("  Node {}: {:?}", node_id, node.node_type);
    //         //
    //         //         println!("{}", serde_json::to_string(&node).unwrap());
    //         //     }
    //         // }
    //         //
    //
    //         println!("{}", serde_json::to_string(&flow).unwrap());
    //
    //         // let db = ReifyDB::embedded_blocking_with(optimistic(memory())).build();
    //
    //         db.command_as_root(
    //             r#"
    //             from[{data: blob::utf8('$REPLACE')}]
    //             insert reifydb.flows
    //         "#
    //             .replace("$REPLACE", serde_json::to_string(&flow).unwrap().as_str())
    //             .as_str(),
    //             Params::None,
    //         )
    //         .unwrap();
    //     }
    //     Err(e) => {
    //         println!("❌ FlowGraph compilation failed: {}", e);
    //     }
    // }

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

    dbg!(&flow);

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
                Diff { changes: vec![Change::Insert { columns }], metadata: Default::default() },
            )
            .unwrap();
        //
    }

    // Query the computed view results
    println!("\n--- Computed View Results ---");
    let results = engine.get_view_data("adults").unwrap();
    let frame = Frame::from(results);
    println!("Adults view contains {} rows:", frame.first().unwrap().data.len());
    println!("{}", frame);

    println!("\nExpected: Only users with age > 18 (Bob: 22, Diana: 25, Eve: 19)");
}
