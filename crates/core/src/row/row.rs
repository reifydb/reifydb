// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Value;
use crate::row::{EncodedRow, Layout};

#[derive(Debug, Clone)]
pub struct Row {
    pub layout: Layout,
    pub data: EncodedRow,
}

impl Row {
    pub fn new(layout: Layout, data: EncodedRow) -> Self {
        Self { layout, data }
    }

    pub fn get(&self, idx: usize) -> crate::Result<Value> {
        Ok(self.layout.get_value(&self.data, idx))
    }
}
