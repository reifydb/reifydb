// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::rql::ast;
use reifydb::rql::ast::{Ast, AstFrom};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Row(pub HashMap<String, String>);

#[derive(Debug)]
pub struct Table {
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

#[derive(Debug)]
pub enum Plan {
    ScanSeq { table_name: String },
    Project { input: Box<Plan>, columns: Vec<String> },
}

pub fn plan(ast: &Ast) -> Result<Plan, String> {
    match ast {
        Ast::Block(nodes) => {
            let mut current: Option<Plan> = None;

            for node in &nodes.nodes {
                match node {
                    Ast::From(from) => {
                        current = Some(plan_from(from)?);
                    }
                    Ast::Select(select) => {
                        let plan = current.unwrap();

                        let mut columns = vec![];
                        for column in &select.columns {
                            match column {
                                Ast::Identifier(node) => {
                                    columns.push(node.value().to_string());
                                }
                                _ => unimplemented!(),
                            }
                        }

                        current = Some(Plan::Project { input: Box::new(plan), columns });
                    }
                    _ => return Err("Unsupported AST node in block".to_string()),
                }
            }

            current.ok_or("Empty block".to_string())
        }
        _ => unimplemented!(),
    }
}

pub fn plan_from(from: &AstFrom) -> Result<Plan, String> {
    dbg!(&from);
    match &*from.source {
        Ast::Identifier(id) => Ok(Plan::ScanSeq { table_name: id.name() }),
        other => unimplemented!("{:?}", other),
    }
}

pub fn execute_plan(plan: &Plan, db: &Database) -> Result<Vec<Row>, String> {
    match plan {
        Plan::ScanSeq { table_name } => {
            let table = db.tables.get(table_name).ok_or("Table not found")?;
            Ok(table.rows.clone())
        }
        Plan::Project { input, columns } => {
            let input_rows = execute_plan(input, db)?;
            if columns.len() == 1 && columns[0] == "*" {
                return Ok(input_rows);
            }

            let result = input_rows
                .into_iter()
                .map(|row| {
                    let filtered = columns.iter().filter_map(|c| row.0.get(c).map(|v| (c.clone(), v.clone()))).collect();
                    Row(filtered)
                })
                .collect();
            Ok(result)
        }
    }
}

fn main() {
    let mut db = Database { tables: HashMap::new() };

    db.tables.insert(
        "users".to_string(),
        Table {
            rows: vec![
                Row(HashMap::from([("id".to_string(), 1.to_string()), ("name".to_string(), "Alice".to_string())])),
                Row(HashMap::from([("id".to_string(), 2.to_string()), ("name".to_string(), "Bob".to_string())])),
            ],
        },
    );

    let mut ast = ast::parse(
        r#"
    FROM users 
    SELECT name
"#,
    );

    // let ast = Ast::Block(AstBlock {
    //     nodes: vec![
    //         Ast::From(AstFrom { source: Box::new(Ast::Identifier(AstIdentifier { name: "users".to_string() })) }),
    //         Ast::Select(AstSelect { columns: vec![Ast::Wildcard] }),
    //     ],
    // });
    // dbg!(&ast);

    let plan = plan(&ast).unwrap();

    let result = execute_plan(&plan, &db).unwrap();
    for row in result {
        println!("{:?}", row);
    }
}
