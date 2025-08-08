// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::row::{EncodedRow, EncodedRowLayout};
use reifydb_core::{RowId, Value};

#[derive(Debug, Clone)]
pub struct Row {
    pub id: RowId,
    pub layout: EncodedRowLayout,
    pub data: EncodedRow,
}

impl Row {
    pub fn new(id: RowId, layout: EncodedRowLayout, data: EncodedRow) -> Self {
        Self { id, layout, data }
    }

    pub fn get(&self, idx: usize) -> crate::Result<Value> {
        Ok(self.layout.get_value(&self.data, idx))
    }
}
