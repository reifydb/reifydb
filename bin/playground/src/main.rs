// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::core::expression::{ConstantExpression, Expression};
use reifydb::core::frame::Frame;
use reifydb::core::interface::{SchemaId, Table, TableId};
use reifydb::core::{OwnedSpan, SpanColumn, SpanLine, Value};
use reifydb::engine::flow::change::{Change, Diff};
use reifydb::engine::flow::engine::FlowEngine;
// use reifydb::engine::flow::change::{Change, Diff};
// use reifydb::engine::flow::engine::FlowEngine;
use reifydb::engine::flow::flow::FlowGraph;
use reifydb::engine::flow::node::{NodeType, OperatorType};
use reifydb::transaction::mvcc::transaction::serializable::Serializable;
use reifydb::{memory, serializable};

fn main() {
    //     let db = ReifyDB::embedded_blocking_with(serializable(memory()));
    //
    //     db.tx_as_root(r#"create schema test"#).unwrap();
    //     db.tx_as_root(r#"create table test.one(field: int1, other: int1)"#).unwrap();
    //     db.tx_as_root(r#"create table test.two(field: int1, name: text)"#).unwrap();
    //     db.tx_as_root(r#"create table test.three(field: int1, type: text)"#).unwrap();
    //     db.tx_as_root(r#"from [{field: 1, other: 2}, {field: 2, other: 2}, {field: 3, other: 2}, {field: 4, other: 2}, {field: 5, other: 2}] insert test.one"#).unwrap();
    //     db.tx_as_root(
    //         r#"from [{field: 2, name: "Peter"}, {field: 5, name: "Parker"}] insert test.two"#,
    //     )
    //     .unwrap();
    //     db.tx_as_root(r#"from [{field: 5, type: "Barker"}] insert test.three"#).unwrap();
    //
    //     for frame in db
    //         .tx_as_root(
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

    // Dataflow example
    dataflow_example();
}

fn dataflow_example() {
    println!("\n=== Dataflow Example ===");

    // Create a simple flow: Table -> Filter -> View
    let mut flow_graph = FlowGraph::new();

    // Create table node
    let table =
        Table { id: TableId(1), schema: SchemaId(1), name: "users".to_string(), columns: vec![] };
    let source_node = flow_graph.add_node(NodeType::Source { name: "users".to_string(), table });

    // Create filter node (filter users with age > 18)
    let filter_expr = Expression::Constant(ConstantExpression::Bool {
        span: OwnedSpan { column: SpanColumn(0), line: SpanLine(0), fragment: "true".to_string() },
    });

    let filter_node = flow_graph
        .add_node(NodeType::Operator { operator: OperatorType::Filter { predicate: filter_expr } });

    // Create view node
    let view_table = Table {
        id: TableId(2),
        schema: SchemaId(1),
        name: "adult_users".to_string(),
        columns: vec![],
    };
    let sink_node =
        flow_graph.add_node(NodeType::Sink { name: "adult_users".to_string(), table: view_table });

    // Connect nodes: source -> filter -> sink
    flow_graph.add_edge(&source_node, &filter_node).unwrap();
    flow_graph.add_edge(&filter_node, &sink_node).unwrap();

    // Create engine and initialize
    // For playground, we'll skip the full transactional engine for now
    // let (versioned, unversioned, hooks) = sqlite(SqliteConfig::new("/tmp/test"));
    let (versioned, unversioned, hooks) = memory();

    let mut engine = FlowEngine::<_, _, Serializable<_, _>>::new(
        flow_graph.clone(),
        serializable((versioned.clone(), unversioned.clone(), hooks)).0,
    );

    engine.initialize().unwrap();

    // Create sample data as frames with rowId and age columns
    let frame = Frame::from_rows(
        &["age"],
        &[vec![Value::Int1(13)], vec![Value::Int1(18)], vec![Value::Int1(25)]],
    );

    println!("Created frame with {} rows and {} columns", frame.row_count(), frame.column_count());

    // Display frame contents
    for i in 0..frame.row_count() {
        let row = frame.get_row(i);
        println!("Row {}: {:?}", i, row);
    }

    engine
        .process_change(
            &source_node,
            Diff { changes: vec![Change::Insert { frame }], metadata: Default::default() },
        )
        .unwrap();

    // Query the view results
    let results = engine.get_view_data("adult_users").unwrap();
    println!("View results: {} rows", results.row_count());
    println!("{}", results);

    println!("Flow graph created with {} nodes", flow_graph.get_all_nodes().count());

    println!("Dataflow example completed!");
}
