// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::FrameColumn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub columns: Vec<FrameColumn>,
}

impl Frame {
    pub fn new(columns: Vec<FrameColumn>) -> Self {
        // let n = columns.first().map_or(0, |c| c.values().len());
        // assert!(columns.iter().all(|c| c.values().len() == n));

        Self { columns }
    }

    // pub fn new_with_name(columns: Vec<FrameColumn>, name: impl Into<String>) -> Self {
    //     // let n = columns.first().map_or(0, |c| c.values().len());
    //     // assert!(columns.iter().all(|c| c.values().len() == n));
    //
    //     Self { name: name.into(), columns }
    // }

    // pub fn shape(&self) -> (usize, usize) {
    //     (self.columns.get(0).map(|c| c.values().len()).unwrap_or(0), self.columns.len())
    // }

    // pub fn is_empty(&self) -> bool {
    //     self.shape().0 == 0
    // }

    // pub fn row(&self, i: usize) -> Vec<Value> {
    //     self.columns.iter().map(|c| c.values().get_value(i)).collect()
    // }
    //
    // pub fn column(&self, name: &str) -> Option<&FrameColumn> {
    //     // Try qualified name first, then try as original name
    //     self.index
    //         .get(name)
    //         .map(|&i| &self.columns[i])
    //         .or_else(|| self.columns.iter().find(|col| col.name() == name))
    // }

    // pub fn column_by_source(&self, frame: &str, name: &str) -> Option<&FrameColumn> {
    //     self.frame_index.get(&(frame.to_string(), name.to_string())).map(|&i| &self.columns[i])
    // }

    // pub fn column_values(&self, name: &str) -> Option<&ColumnValues> {
    //     // Try qualified name first, then try as original name
    //     self.index
    //         .get(name)
    //         .map(|&i| self.columns[i].values())
    //         .or_else(|| self.columns.iter().find(|col| col.name() == name).map(|col| col.values()))
    // }
    //
    // pub fn column_values_mut(&mut self, name: &str) -> Option<&mut ColumnValues> {
    //     // Try qualified name first, then try as original name
    //     if let Some(&i) = self.index.get(name) {
    //         Some(self.columns[i].values_mut())
    //     } else {
    //         let pos = self.columns.iter().position(|col| col.name() == name)?;
    //         Some(self.columns[pos].values_mut())
    //     }
    // }

    // pub fn row_count(&self) -> usize {
    //     self.columns.first().map_or(0, |col| col.values().len())
    // }
    //
    // pub fn column_count(&self) -> usize {
    //     self.columns.len()
    // }
    //
    // pub fn get_row(&self, index: usize) -> Vec<Value> {
    //     self.columns.iter().map(|col| col.values().get_value(index)).collect()
    // }
}
