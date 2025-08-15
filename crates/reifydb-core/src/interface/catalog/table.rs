// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{
	Type,
	interface::{
		ColumnIndex, ColumnPolicy, SchemaId, TableColumnId, TableId,
	},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableColumnDef {
	pub id: TableColumnId,
	pub name: String,
	pub ty: Type,
	pub policies: Vec<ColumnPolicy>,
	pub index: ColumnIndex,
	pub auto_increment: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
	pub id: TableId,
	pub schema: SchemaId,
	pub name: String,
	pub columns: Vec<TableColumnDef>,
}
