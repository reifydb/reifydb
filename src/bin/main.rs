// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::rql::ast::{Ast, AstBlock, AstExpression, AstFrom, AstIdentifier, AstSelect};
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
    Scan { table_name: String },
    Project { input: Box<Plan>, columns: Vec<String> },
}

pub fn plan(ast: &Ast) -> Result<Plan, String> {
    match ast {
        Ast::Block(nodes) => {
            let mut current: Option<Plan> = None;

            for node in &nodes.nodes {
                match node {
                    Ast::From(from) => {
                        current = Some(plan(&Ast::From(from.clone()))?);
                    }
                    Ast::Select(select) => {
                        let input = current.ok_or("SELECT without FROM")?;
                        current = Some(Plan::Project { input: Box::new(input), columns: vec!["name".to_string()] });
                    }
                    _ => return Err("Unsupported AST node in block".to_string()),
                }
            }

            current.ok_or("Empty block".to_string())
        }

        Ast::From(from) => match &*from.source {
            Ast::Identifier(id) => Ok(Plan::Scan { table_name: id.name.clone() }),
            other => plan(other),
        },

        Ast::Select(_) => Err("Select must appear in block with FROM".to_string()),

        Ast::Identifier(_) => Err("Unexpected Identifier".to_string()),
        Ast::Literal(_) => Err("Literal not implemented yet".to_string()),
        Ast::Where(_) => Err("Where not implemented yet".to_string()),
        Ast::Expression(_) => Err("Expression not implemented yet".to_string()),
    }
}

pub fn execute_plan(plan: &Plan, db: &Database) -> Result<Vec<Row>, String> {
    match plan {
        Plan::Scan { table_name } => {
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

    let ast = Ast::Block(AstBlock {
        nodes: vec![
            Ast::From(AstFrom { alias: None, source: Box::new(Ast::Identifier(AstIdentifier { name: "users".to_string() })) }),
            Ast::Select(AstSelect { columns: vec![Ast::Expression(AstExpression::All)] }),
        ],
    });
    dbg!(&ast);

    let plan = plan(&ast).unwrap();
    dbg!(&plan);

    let result = execute_plan(&plan, &db).unwrap();
    for row in result {
        println!("{:?}", row);
    }
}
