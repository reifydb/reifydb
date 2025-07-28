// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::core::expression::{ConstantExpression, Expression};
use reifydb::core::flow::change::{Change, Diff};
use reifydb::core::flow::engine::FlowEngine;
use reifydb::core::flow::flow::FlowGraph;
use reifydb::core::flow::node::{NodeType, OperatorType};
use reifydb::core::frame::Frame;
use reifydb::core::interface::{SchemaId, Table, TableId};
use reifydb::core::{OwnedSpan, RowId, SpanColumn, SpanLine, Value};

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
    let table_node = flow_graph.add_node(NodeType::Table { name: "users".to_string(), table });

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
    let view_node =
        flow_graph.add_node(NodeType::View { name: "adult_users".to_string(), table: view_table });

    // Connect nodes: table -> filter -> view
    flow_graph.add_edge(&table_node, &filter_node).unwrap();
    flow_graph.add_edge(&filter_node, &view_node).unwrap();

    // Create engine and initialize
    let mut engine = FlowEngine::new(flow_graph);
    engine.initialize().unwrap();

    // Create sample data as frames with rowId and age columns
    let frame = Frame::from_rows(
        &["__ROW_ID__", "age"],
        &[
            vec![Value::RowId(RowId(1)), Value::Int1(13)],
            vec![Value::RowId(RowId(2)), Value::Int1(18)],
            vec![Value::RowId(RowId(3)), Value::Int1(25)],
        ],
    );

    println!("Created frame with {} rows and {} columns", frame.row_count(), frame.column_count());

    // Display frame contents
    for i in 0..frame.row_count() {
        let row = frame.get_row(i);
        println!("Row {}: {:?}", i, row);
    }

    engine
        .process_change(
            &table_node,
            Diff { changes: vec![Change::Insert { frame }], metadata: Default::default() },
        )
        .unwrap();

    // Query the view results
    let results = engine.get_view_data("adult_users").unwrap();
    println!("View results: {} rows", results.len());

    for (i, row) in results.iter().enumerate() {
        println!("Row {}: {} bytes", i, row.data.len());
    }

    println!("Dataflow example completed!");
}
