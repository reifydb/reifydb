// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{Column, SchemaId, TableId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
    pub columns: Vec<Column>,
}
