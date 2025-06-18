// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::iterator::FrameIter;
use crate::frame::{Column, ColumnValues, ValueRef};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Frame {
    pub columns: Vec<Column>,
    pub index: HashMap<String, usize>,
}

impl Frame {
    pub fn new(columns: Vec<Column>) -> Self {
        let n = columns.first().map_or(0, |c| c.data.len());
        assert!(columns.iter().all(|c| c.data.len() == n));

        let index = columns.iter().enumerate().map(|(i, col)| (col.name.clone(), i)).collect();

        Self { columns, index }
    }

    pub fn empty() -> Self {
        Self { columns: vec![], index: HashMap::new() }
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.columns.get(0).map(|c| c.data.len()).unwrap_or(0), self.columns.len())
    }

    pub fn is_empty(&self) -> bool {
        self.shape().0 == 0
    }

    pub fn row(&self, i: usize) -> Vec<ValueRef> {
        self.columns.iter().map(|c| c.data.get(i)).collect()
    }

    pub fn column(&self, name: &str) -> Option<&ColumnValues> {
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
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
