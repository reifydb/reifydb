// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use layout::FrameColumnLayout;
use serde::{Deserialize, Serialize};
pub use values::ColumnValues;
use crate::Type;

mod layout;
mod values;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameColumn {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub name: String,
    pub values: ColumnValues,
}

impl FrameColumn {
    pub fn qualified_name(&self) -> String {
        match (&self.schema, &self.table) {
            (Some(schema), Some(table)) => format!("{}.{}.{}", schema, table, self.name),
            (None, Some(table)) => format!("{}.{}", table, self.name),
            _ => self.name.clone(),
        }
    }

    pub fn get_type(&self) -> Type {
        self.values.get_type()
    }
}
