// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::iterator::FrameIter;
use crate::frame::{Column, ColumnValues};
use reifydb_core::Kind::Undefined;
use reifydb_core::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Frame {
    pub name: String,
    pub columns: Vec<Column>,
    pub index: HashMap<String, usize>,
}

impl Frame {
    pub fn new(columns: Vec<Column>) -> Self {
        let n = columns.first().map_or(0, |c| c.data.len());
        assert!(columns.iter().all(|c| c.data.len() == n));

        let index = columns.iter().enumerate().map(|(i, col)| (col.name.clone(), i)).collect();

        Self { name: "frame".to_string(), columns, index }
    }

    pub fn new_with_name(columns: Vec<Column>, name: impl Into<String>) -> Self {
        let n = columns.first().map_or(0, |c| c.data.len());
        assert!(columns.iter().all(|c| c.data.len() == n));

        let index = columns.iter().enumerate().map(|(i, col)| (col.name.clone(), i)).collect();

        Self { name: name.into(), columns, index }
    }

    pub fn empty() -> Self {
        Self { name: "frame".to_string(), columns: vec![], index: HashMap::new() }
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

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.index.get(name).map(|&i| &self.columns[i])
    }

    pub fn column_values(&self, name: &str) -> Option<&ColumnValues> {
        self.index.get(name).map(|&i| &self.columns[i].data)
    }

    pub fn iter(&self) -> FrameIter<'_> {
        let col_names = self.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
        FrameIter {
            df: self,
            row_index: 0,
            row_total: self.shape().0,
            column_index: Arc::new(col_names),
        }
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |col| col.data.len())
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_row(&self, index: usize) -> Vec<Value> {
        self.columns.iter().map(|col| col.data.get(index)).collect()
    }
}

impl Frame {
    pub fn qualify_columns(&mut self) {
        for col in &mut self.columns {
            col.name = format!("{}_{}", self.name, col.name);
        }
    }
}

impl Frame {
    pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
        let column_count = names.len();

        let mut columns: Vec<Column> = names
            .iter()
            .map(|name| Column {
                name: name.to_string(),
                data: ColumnValues::with_capacity(Undefined, 0),
            })
            .collect();

        for row in result_rows {
            assert_eq!(row.len(), column_count, "row length does not match column count");
            for (i, value) in row.iter().enumerate() {
                columns[i].data.push_value(value.clone());
            }
        }

        Frame::new(columns)
    }
}
