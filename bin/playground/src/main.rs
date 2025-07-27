// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::core::expression::{ConstantExpression, Expression};
use reifydb::core::flow::change::{Change, Diff};
use reifydb::core::flow::engine::FlowEngine;
use reifydb::core::flow::flow::FlowGraph;
use reifydb::core::flow::node::{NodeType, OperatorType};
use reifydb::core::flow::row::Row;
use reifydb::core::interface::{SchemaId, Table, TableId};
use reifydb::core::row::Layout;
use reifydb::core::{OwnedSpan, RowId, SpanColumn, SpanLine, Type};

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

    // Create sample data
    let layout = Layout::new(&[Type::Int1]);

    let mut user1 = layout.allocate_row();
    layout.set_i8(&mut user1, 0, 13);

    let mut user2 = layout.allocate_row();
    layout.set_i8(&mut user2, 0, 18);

    let mut user3 = layout.allocate_row();
    layout.set_i8(&mut user3, 0, 25);

    // Create change with sample inserts
    let change = Diff::new(vec![
        Change::Insert { row: Row::new(RowId(1), layout.clone(), user1) },
        Change::Insert { row: Row::new(RowId(2), layout.clone(), user2) },
        Change::Insert { row: Row::new(RowId(3), layout.clone(), user3) },
    ]);

    // Process the change through the dataflow
    println!("Processing change through dataflow...");
    engine.process_change(&table_node, change).unwrap();

    // Query the view results
    let results = engine.get_view_data("adult_users").unwrap();
    println!("View results: {} rows", results.len());

    for (i, row) in results.iter().enumerate() {
        println!("Row {}: {} bytes", i, row.data.len());
    }

    println!("Dataflow example completed!");
}
