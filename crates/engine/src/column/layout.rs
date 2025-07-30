// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::FrameColumn;

#[derive(Debug, Clone)]
pub struct FrameColumnLayout {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub name: String,
}

impl FrameColumnLayout {
    pub fn from_column(column: &FrameColumn) -> Self {
        Self {
            schema: column.schema().map(|s| s.to_string()),
            table: column.table().map(|s| s.to_string()),
            name: column.name().to_string(),
        }
    }
}
