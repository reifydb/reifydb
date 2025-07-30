// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::frame::FrameColumn;

#[derive(Debug, Clone)]
pub struct FrameColumnLayout {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub name: String,
}

impl FrameColumnLayout {
    pub fn from_column(column: &FrameColumn) -> Self {
        Self {
            schema: column.schema.clone(),
            table: column.table.clone(),
            name: column.name.clone(),
        }
    }
}
