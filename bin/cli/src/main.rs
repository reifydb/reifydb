// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::rql::ast;
use reifydb::rql::plan::node::Node;
use reifydb::rql::plan::{plan, Plan};
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

// #[derive(Debug)]
// pub enum Plan {
//     ScanSeq { table_name: String },
//     Project { input: Box<Plan>, columns: Vec<String> },
// }

// pub fn plan(ast: &Ast) -> Result<Plan, String> {
//     match ast {
//         Ast::Block(nodes) => {
//             let mut current: Option<Plan> = None;
//
//             for node in &nodes.nodes {
//                 match node {
//                     Ast::From(from) => {
//                         current = Some(plan_from(from)?);
//                     }
//                     Ast::Select(select) => {
//                         let plan = current.unwrap();
//
//                         let mut columns = vec![];
//                         for column in &select.columns {
//                             match column {
//                                 Ast::Identifier(node) => {
//                                     columns.push(node.value().to_string());
//                                 }
//                                 _ => unimplemented!(),
//                             }
//                         }
//
//                         current = Some(Plan::Project { input: Box::new(plan), columns });
//                     }
//                     _ => return Err("Unsupported AST node in block".to_string()),
//                 }
//             }
//
//             current.ok_or("Empty block".to_string())
//         }
//         _ => unimplemented!(),
//     }
// }

// pub fn plan_from(from: &AstFrom) -> Result<Plan, String> {
//     dbg!(&from);
//     match &*from.source {
//         Ast::Identifier(id) => Ok(Plan::ScanSeq { table_name: id.name() }),
//         other => unimplemented!("{:?}", other),
//     }
// }

pub fn execute_plan(plan: &Plan, db: &Database) -> Result<Vec<Row>, String> {
    match plan {
        Plan::Query { node } => execute_node(node, db),
    }
}

pub fn execute_node(node: &Node, db: &Database) -> Result<Vec<Row>, String> {
    match node {
        Node::Project { input, .. } => {
            let input_rows = execute_node(input, db)?;
            // if columns.len() == 1 && columns[0] == "*" {
            //     return Ok(input_rows);
            // }

            let columns = vec!["id".to_string(), "name".to_string()];

            let result = input_rows
                .into_iter()
                .map(|row| {
                    let filtered = columns
                        .iter()
                        .filter_map(|c| row.0.get(c).map(|v| (c.clone(), v.clone())))
                        .collect();
                    Row(filtered)
                })
                .collect();
            Ok(result)
        }
        Node::Scan { .. } => {
            let table_name = "users";
            let table = db.tables.get(table_name).ok_or("Table not found")?;
            Ok(table.rows.clone())
        }
    }
}

pub struct CursorIter<I>
where
    I: Iterator,
{
    iter: I,
}

impl<I> CursorIter<I>
where
    I: Iterator,
{
    pub fn new(iter: I) -> Self {
        Self { iter }
    }

    pub fn map<B, M>(self, mut f: M) -> CursorIter<impl Iterator<Item = B>>
    where
        M: FnMut(I::Item) -> B,
    {
        let iter = self.iter.map(move |item| {
            let mapped = f(item);
            mapped
        });
        CursorIter::new(iter)
    }

    pub fn filter<P>(self, mut predicate: P) -> CursorIter<impl Iterator<Item = I::Item>>
    where
        P: FnMut(&I::Item) -> bool,
    {
        let iter = self.iter.filter(move |item| predicate(item));
        CursorIter::new(iter)
    }

    pub fn take(self, n: usize) -> CursorIter<impl Iterator<Item = I::Item>> {
        CursorIter::new(self.iter.take(n))
    }

    pub fn interleave<J>(self, other: J) -> CursorIter<impl Iterator<Item = I::Item>>
    where
        J: IntoIterator<Item = I::Item>,
        I: Iterator,
    {
        let a = self.iter;
        let b = other.into_iter();

        let iter = a.zip(b).flat_map(|(x, y)| vec![x, y]);

        CursorIter::new(iter)
    }

    pub fn zip<J>(
        self,
        other: CursorIter<J>,
    ) -> CursorIter<impl Iterator<Item = (I::Item, J::Item)>>
    where
        J: Iterator,
    {
        CursorIter::new(self.iter.zip(other.iter))
    }
}

impl<I> Iterator for CursorIter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        Some(item)
    }
}

#[derive(Debug, Clone)]
struct CursorRow {
    id: u32,
    data: String,
}

/// Simulates a database function that loads a page of rows
fn load_rows(offset: usize, limit: usize) -> Vec<CursorRow> {
    // Simulate a large DB with 100 rows
    let total_rows = 10000000;
    let end = (offset + limit).min(total_rows);

    (offset + 1..end + 1)
        .map(|i| CursorRow { id: i as u32, data: format!("data_{}", i) })
        .collect::<Vec<_>>()
}

struct Cursor {
    offset: usize,
    buffer: Vec<CursorRow>,
    buffer_pos: usize,
    chunk_size: usize,
    done: bool,
}

impl Cursor {
    fn new(chunk_size: usize) -> Self {
        Self { offset: 0, buffer: Vec::new(), buffer_pos: 0, chunk_size, done: false }
    }

    fn fetch_next_chunk(&mut self) {
        self.buffer = load_rows(self.offset, self.chunk_size);
        self.offset += self.buffer.len();
        self.buffer_pos = 0;
        if self.buffer.is_empty() {
            self.done = true;
        }
    }
}

impl Iterator for Cursor {
    type Item = CursorRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if self.buffer_pos >= self.buffer.len() {
            self.fetch_next_chunk();
            if self.done {
                return None;
            }
        }

        let item = self.buffer[self.buffer_pos].clone();
        self.buffer_pos += 1;
        Some(item)
    }
}

fn main() {
    // let other = vec![1, 2, 3, 4, 5, 6];
    // let data = vec!["a"; 3000];
    //
    // let cursor = Cursor::new(10000); // Read in chunks of 10 rows
    //
    // let x = CursorIter::new(data.into_iter());
    // let y = CursorIter::new(cursor);
    //
    // let source = y.filter(|(x)| x.id % 40 == 0);
    //
    // for (x, y) in source.zip(x).take(20
    // ) {
    //     println!("{} {}", x.id, y);
    // }
    //
    //

    // let reactive = ReactiveIter::new(data.into_iter());
    //
    // let result: Vec<_> = reactive
    //     .filter(|x| x % 2 == 0)
    //     .map(|x| x * 10)
    //     .take(2)
    //     .collect();
    //
    // println!("Final: {:?}", result);

    let mut db = Database { tables: HashMap::new() };

    db.tables.insert(
        "users".to_string(),
        Table {
            rows: vec![
                Row(HashMap::from([
                    ("id".to_string(), 1.to_string()),
                    ("name".to_string(), "Alice".to_string()),
                ])),
                Row(HashMap::from([
                    ("id".to_string(), 2.to_string()),
                    ("name".to_string(), "Bob".to_string()),
                ])),
            ],
        },
    );

    let mut ast = ast::parse(
        r#"
        FROM users
        SELECT name
    "#,
    );

    let plan = plan(ast).unwrap();

    //
    //     let plan = plan(&ast).unwrap();
    //
    let result = execute_plan(&plan, &db).unwrap();
    for row in result {
        println!("{:?}", row);
    }
}
