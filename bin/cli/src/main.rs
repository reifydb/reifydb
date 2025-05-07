// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::rql::plan::{QueryPlan, plan};
use reifydb::rql::{Expression, ast};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Row(pub HashMap<String, String>);

#[derive(Debug)]
pub struct Table {
    pub rows: Vec<Row>,
}

impl Table {
    pub fn scan(&self) -> impl Iterator<Item = Row> + '_ {
        self.rows.iter().cloned()
    }
}

#[derive(Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

pub fn execute_plan(plan: &QueryPlan, db: &Database) -> Result<Vec<Row>, String> {
    let iter = execute_node(plan, db, None)?;
    Ok(iter.collect())
}

fn execute_node<'a>(
    node: &'a QueryPlan,
    db: &'a Database,
    input: Option<Box<dyn Iterator<Item = Row> + 'a>>,
) -> Result<Box<dyn Iterator<Item = Row> + 'a>, String> {
    let result_iter: Box<dyn Iterator<Item = Row> + 'a> = match node {
        QueryPlan::Scan { source, next } => {
            let table = db.tables.get(source).ok_or("Table not found")?;
            Box::new(table.scan())
        }
        QueryPlan::Limit { limit: count, next } => {
            let input_iter = input.ok_or("Missing input for Project")?;
            Box::new(input_iter.take(*count))
        }
        QueryPlan::Project { expressions, next } => {
            let input_iter = input.ok_or("Missing input for Project")?;
            let columns: Vec<String> = expressions
                .iter()
                .filter_map(|expr| match expr {
                    Expression::Identifier(name) => Some(name.clone()),
                    _ => None,
                })
                .collect();
            Box::new(input_iter.map(move |row| {
                let filtered = columns
                    .iter()
                    .filter_map(|c| row.0.get(c).map(|v| (c.clone(), v.clone())))
                    .collect();
                Row(filtered)
            }))
        } // Add more cases (Filter, OrderBy, Limit) here
    };

    if let Some(next_node) = match node {
        QueryPlan::Scan { next, .. } | QueryPlan::Project { next, .. } => next.as_deref(),
        QueryPlan::Limit { next, .. } => next.as_deref(),
    } {
        execute_node(next_node, db, Some(result_iter))
    } else {
        Ok(result_iter)
    }
}

pub struct RowIter<I>
where
    I: Iterator,
{
    iter: I,
}

impl<I> RowIter<I>
where
    I: Iterator,
{
    pub fn new(iter: I) -> Self {
        Self { iter }
    }

    pub fn map<B, M>(self, mut f: M) -> RowIter<impl Iterator<Item = B>>
    where
        M: FnMut(I::Item) -> B,
    {
        let iter = self.iter.map(move |item| {
            let mapped = f(item);
            mapped
        });
        RowIter::new(iter)
    }

    pub fn filter<P>(self, mut predicate: P) -> RowIter<impl Iterator<Item = I::Item>>
    where
        P: FnMut(&I::Item) -> bool,
    {
        let iter = self.iter.filter(move |item| predicate(item));
        RowIter::new(iter)
    }

    pub fn take(self, n: usize) -> RowIter<impl Iterator<Item = I::Item>> {
        RowIter::new(self.iter.take(n))
    }

    pub fn interleave<J>(self, other: J) -> RowIter<impl Iterator<Item = I::Item>>
    where
        J: IntoIterator<Item = I::Item>,
        I: Iterator,
    {
        let a = self.iter;
        let b = other.into_iter();

        let iter = a.zip(b).flat_map(|(x, y)| vec![x, y]);

        RowIter::new(iter)
    }

    pub fn zip<J>(self, other: RowIter<J>) -> RowIter<impl Iterator<Item = (I::Item, J::Item)>>
    where
        J: Iterator,
    {
        RowIter::new(self.iter.zip(other.iter))
    }
}

impl<I> Iterator for RowIter<I>
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

    let mut statements = ast::parse(
        r#"
        FROM users
        SELECT name
        LIMIT 100
    "#,
    );

    for statement in statements {
        let plan = plan(statement).unwrap();

        let result = execute_plan(&plan, &db).unwrap();
        for row in result {
            println!("{:?}", row);
        }
    }
}
