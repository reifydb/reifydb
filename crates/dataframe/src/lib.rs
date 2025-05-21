// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
#![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum Value {
    Int(i64),
    Float(OrderedF64),
    Text(String),
    Bool(bool),
    Undefined,
}

#[derive(Debug, Clone)]
pub enum ColumnValues {
    Int(Vec<i64>, Vec<bool>), // value, is_valid
    Float(Vec<f64>, Vec<bool>),
    Text(Vec<String>, Vec<bool>),
    Bool(Vec<bool>, Vec<bool>),
    Undefined(usize), // special case: all undefined
}
impl ColumnValues {
    pub fn get(&self, index: usize) -> Value {
        match self {
            ColumnValues::Int(v, b) => {
                if b[index] {
                    Value::Int(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Float(v, b) => {
                if b[index] {
                    Value::Float(OrderedF64(v[index]))
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Text(v, b) => {
                if b[index] {
                    Value::Text(v[index].clone())
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Bool(v, b) => {
                if b[index] {
                    Value::Bool(v[index])
                } else {
                    Value::Undefined
                }
            }
            ColumnValues::Undefined(_) => Value::Undefined,
        }
    }

    pub fn push(&mut self, value: Value) {
        match (self, value) {
            (ColumnValues::Int(v, b), Value::Int(i)) => {
                v.push(i);
                b.push(true);
            }
            (ColumnValues::Float(v, b), Value::Float(f)) => {
                v.push(f.0);
                b.push(true);
            }
            (ColumnValues::Text(v, b), Value::Text(s)) => {
                v.push(s);
                b.push(true);
            }
            (ColumnValues::Bool(v, b), Value::Bool(x)) => {
                v.push(x);
                b.push(true);
            }
            (ColumnValues::Int(_, b), Value::Undefined)
            | (ColumnValues::Float(_, b), Value::Undefined)
            | (ColumnValues::Text(_, b), Value::Undefined)
            | (ColumnValues::Bool(_, b), Value::Undefined) => b.push(false),
            (ColumnValues::Undefined(n), Value::Undefined) => *n += 1,
            _ => panic!("Mismatched column type and value"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Int(_, b) => b.len(),
            ColumnValues::Float(_, b) => b.len(),
            ColumnValues::Text(_, b) => b.len(),
            ColumnValues::Bool(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }

    pub fn is_undefined(&self, index: usize) -> bool {
        match self {
            ColumnValues::Int(_, b)
            | ColumnValues::Float(_, b)
            | ColumnValues::Text(_, b)
            | ColumnValues::Bool(_, b) => !b[index],
            ColumnValues::Undefined(_) => true,
        }
    }

    pub fn empty(&self) -> ColumnValues {
        match self {
            ColumnValues::Int(_, _) => ColumnValues::Int(Vec::new(), Vec::new()),
            ColumnValues::Float(_, _) => ColumnValues::Float(Vec::new(), Vec::new()),
            ColumnValues::Text(_, _) => ColumnValues::Text(Vec::new(), Vec::new()),
            ColumnValues::Bool(_, _) => ColumnValues::Bool(Vec::new(), Vec::new()),
            ColumnValues::Undefined(_) => ColumnValues::Undefined(0),
        }
    }
}

use base::ordered_float::OrderedF64;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}

#[derive(Debug)]
pub struct DataFrame {
    pub columns: Vec<Column>,
    pub column_index: HashMap<String, usize>,
    pub row_index: Vec<String>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>, index: Vec<String>) -> Self {
        let n = columns.first().map_or(0, |c| c.data.len());
        assert!(columns.iter().all(|c| c.data.len() == n));
        assert_eq!(index.len(), n);

        let col_map = columns.iter().enumerate().map(|(i, col)| (col.name.clone(), i)).collect();

        Self { columns, column_index: col_map, row_index: index }
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.row_index.len(), self.columns.len())
    }

    pub fn row(&self, i: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c.data.get(i)).collect()
    }

    pub fn column(&self, name: &str) -> Option<&ColumnValues> {
        self.column_index.get(name).map(|&i| &self.columns[i].data)
    }

    pub fn select_columns(&self, names: &[&str]) -> Self {
        let selected: Vec<Column> = names
            .iter()
            .filter_map(|&name| self.column_index.get(name).map(|&i| self.columns[i].clone()))
            .collect();

        DataFrame::new(selected, self.row_index.clone())
    }

    pub fn filter_rows<F>(&self, predicate: F) -> Self
    where
        F: Fn(&[Value]) -> bool,
    {
        let mut new_cols: Vec<Column> = self
            .columns
            .iter()
            .map(|col| Column { name: col.name.clone(), data: col.data.empty() })
            .collect();

        let mut new_index = vec![];

        for i in 0..self.row_index.len() {
            let row: Vec<Value> = self.columns.iter().map(|col| col.data.get(i)).collect();

            if predicate(&row) {
                for (col, new_col) in self.columns.iter().zip(new_cols.iter_mut()) {
                    new_col.data.push(col.data.get(i));
                }
                new_index.push(self.row_index[i].clone());
            }
        }

        DataFrame::new(new_cols, new_index)
    }

    pub fn iter(&self) -> DataFrameIter<'_> {
        let col_names = self.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
        DataFrameIter { df: self, row_index: 0, col_names }
    }
}

pub struct RowRef<'df, 'names> {
    pub values: Vec<ValueRef<'df>>,
    pub col_names: &'names [String],
    pub col_map: &'df HashMap<String, usize>,
}

impl<'df, 'names> RowRef<'df, 'names> {
    pub fn get(&self, name: &str) -> Option<&ValueRef<'df>> {
        self.col_map.get(name).and_then(|&i| self.values.get(i))
    }
}

#[derive(Debug, Clone)]
pub enum ValueRef<'a> {
    Int(&'a i64),
    Float(&'a f64),
    Text(&'a str),
    Bool(&'a bool),
    Undefined,
}

impl<'a> ValueRef<'a> {
    pub fn as_value(&self) -> Value {
        match self {
            ValueRef::Int(v) => Value::Int(**v),
            ValueRef::Float(v) => Value::Float(OrderedF64(**v)),
            ValueRef::Text(s) => Value::Text(s.to_string()),
            ValueRef::Bool(b) => Value::Bool(**b),
            ValueRef::Undefined => Value::Undefined,
        }
    }
}

pub struct DataFrameIter<'df> {
    df: &'df DataFrame,
    row_index: usize,
    col_names: Vec<String>,
}

impl<'df> Iterator for DataFrameIter<'df> {
    type Item = RowRef<'df, 'df>; // fix: both lifetimes are 'df for simplicity

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_index >= self.df.row_index.len() {
            return None;
        }

        let i = self.row_index;
        self.row_index += 1;

        let values = self
            .df
            .columns
            .iter()
            .map(|col| match &col.data {
                ColumnValues::Int(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Int(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Float(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Float(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Text(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Text(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Bool(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Bool(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Undefined(_) => ValueRef::Undefined,
            })
            .collect();

        // SAFETY: we trick the borrow checker here a bit with a cast.
        // Because `self.col_names` is owned by the iterator, we need to coerce its lifetime upward.
        // For real safety and ergonomics, use Arc<Vec<String>> instead.

        let col_names: &'df [String] = unsafe { std::mem::transmute(&self.col_names[..]) };

        Some(RowRef { values, col_names, col_map: &self.df.column_index })
    }
}

pub fn inner_join_indices(lhs: &DataFrame, rhs: &DataFrame, on: &str) -> Vec<(usize, usize)> {
    let mut rhs_index: HashMap<Value, Vec<usize>> = HashMap::new();

    let start = Instant::now();
    for (i, row) in rhs.iter().enumerate() {
        if let Some(k) = row.get(on) {
            rhs_index.entry(k.as_value()).or_default().push(i);
        }
    }
    println!("1 took {:?}", start.elapsed());

    let mut joined = vec![];

    let start = Instant::now();
    for (li, lrow) in lhs.iter().enumerate() {
        if let Some(lkey) = lrow.get(on) {
            if let Some(matches) = rhs_index.get(&lkey.as_value()) {
                for &ri in matches {
                    joined.push((li, ri));
                }
            }
        }
    }

    println!("2 took {:?}", start.elapsed());

    joined
}

#[cfg(test)]
mod tests {
	use crate::{inner_join_indices, Column, ColumnValues, DataFrame, ValueRef};
	use rand::rngs::StdRng;
	use rand::{Rng, SeedableRng};
	use std::time::Instant;

	fn generate_large_dataframe() -> DataFrame {
        const N: usize = 1_000_000;
        let mut rng = StdRng::seed_from_u64(42); // deterministic for testing

        let mut ids = Vec::with_capacity(N);
        let mut id_valids = Vec::with_capacity(N);

        let mut scores = Vec::with_capacity(N);
        let mut score_valids = Vec::with_capacity(N);

        let mut passed = Vec::with_capacity(N);
        let mut passed_valids = Vec::with_capacity(N);

        let mut index = Vec::with_capacity(N);

        for i in 0..N {
            let id = (i + 1) as i64;
            let score = (id % 100) as f64 + rng.gen_range(0.0..1.0);
            let pass = score > 50.0;

            ids.push(id);
            id_valids.push(true);

            scores.push(score);
            score_valids.push(true);

            passed.push(pass);
            passed_valids.push(true);

            index.push(format!("row{}", i));
        }

        DataFrame::new(
            vec![
                Column { name: "id".into(), data: ColumnValues::Int(ids, id_valids) },
                Column { name: "score".into(), data: ColumnValues::Float(scores, score_valids) },
                Column { name: "passed".into(), data: ColumnValues::Bool(passed, passed_valids) },
            ],
            index,
        )
    }

    #[test]
    fn test() {
        let df = generate_large_dataframe();

        let start = Instant::now();

        let mut result = df
            .iter()
            .filter(|row| match row.values[1] {
                ValueRef::Float(f) if *f > 60.0 => true,
                _ => false,
            })
            .map(|row| {
                // Project: (id, score)
                (row.values[0].as_value(), row.values[1].as_value())
            })
            .collect::<Vec<_>>();

        println!("{:?}", start.elapsed());

        for (id, score) in result.iter().take(10) {
            println!("id = {:?}, score = {:?}", id, score);
        }
    }

    #[test]
    fn join() {
        // Define left table
        // let left = DataFrame::new(
        //     vec![
        //         Column { name: "id".into(), data: ColumnData::Int(vec![1, 2, 3], vec![true; 3]) },
        //         Column {
        //             name: "name".into(),
        //             data: ColumnData::Text(
        //                 vec!["Alice".into(), "Bob".into(), "Carol".into()],
        //                 vec![true; 3],
        //             ),
        //         },
        //     ],
        //     vec!["row0".into(), "row1".into(), "row2".into()],
        // );
        let left = generate_large_dataframe();

        // Define right table
        let right = DataFrame::new(
            vec![
                Column { name: "id".into(), data: ColumnValues::Int(vec![2, 3, 4], vec![true; 3]) },
                Column {
                    name: "score".into(),
                    data: ColumnValues::Float(vec![90.0, 75.5, 60.0], vec![true; 3]),
                },
            ],
            vec!["r0".into(), "r1".into(), "r2".into()],
        );

        let start = Instant::now();
        // Join on "id"
        let joined_indices = inner_join_indices(&left, &right, "id");

        dbg!(&joined_indices);

        println!("Join result:");
        for (li, ri) in joined_indices {
            let lrow = left.iter().nth(li).unwrap();
            let rrow = right.iter().nth(ri).unwrap();

            let id = lrow.get("id").unwrap();
            let name = lrow.get("passed").unwrap();
            let score = rrow.get("score").unwrap();

            println!("id: {:?}, name: {:?}, score: {:?}", id, name, score);
        }

        println!("took {:?}", start.elapsed());
    }
}
