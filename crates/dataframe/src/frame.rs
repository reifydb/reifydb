// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::iterator::DataFrameIter;
use crate::{Column, ColumnValues};
use base::Value;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DataFrame {
    pub columns: Vec<Column>,
    pub index: HashMap<String, usize>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>) -> Self {
        let n = columns.first().map_or(0, |c| c.data.len());
        assert!(columns.iter().all(|c| c.data.len() == n));

        let index = columns.iter().enumerate().map(|(i, col)| (col.name.clone(), i)).collect();

        Self { columns, index }
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.columns.get(0).map(|c| c.data.len()).unwrap_or(0), self.columns.len())
    }

    pub fn is_empty(&self) -> bool {
        self.shape().0 == 0
    }

    pub fn row(&self, i: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c.data.get(i)).collect()
    }

    pub fn column(&self, name: &str) -> Option<&ColumnValues> {
        self.index.get(name).map(|&i| &self.columns[i].data)
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

        for i in 0..self.shape().0 {
            let row: Vec<Value> = self.columns.iter().map(|col| col.data.get(i)).collect();

            if predicate(&row) {
                for (col, new_col) in self.columns.iter().zip(new_cols.iter_mut()) {
                    new_col.data.push(col.data.get(i));
                }
            }
        }

        DataFrame::new(new_cols)
    }

    pub fn iter(&self) -> DataFrameIter<'_> {
        let col_names = self.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
        DataFrameIter { df: self, row_index: 0, row_total: self.shape().0, colum_names: col_names }
    }
}
