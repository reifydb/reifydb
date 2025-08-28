// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnIndex, PrimaryKeyId, TableColumnDef};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TablePrimaryKeyDef {
	pub id: PrimaryKeyId,
	pub columns: Vec<TableColumnDef>,
}
