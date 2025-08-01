// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::data::FrameColumnData;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameColumn {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub name: String,
    pub data: FrameColumnData,
}

impl Deref for FrameColumn {
    type Target = FrameColumnData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl FrameColumn {
    pub fn qualified_name(&self) -> String {
        match (&self.schema, &self.table) {
            (Some(schema), Some(table)) => format!("{}.{}.{}", schema, table, self.name),
            (None, Some(table)) => format!("{}.{}", table, self.name),
            _ => self.name.clone(),
        }
    }
}
