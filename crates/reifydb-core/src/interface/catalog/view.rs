// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{
	Type,
	interface::{ColumnIndex, SchemaId, ViewColumnId, ViewId},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewColumnDef {
	pub id: ViewColumnId,
	pub name: String,
	pub ty: Type,
	pub index: ColumnIndex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewDef {
	pub id: ViewId,
	pub schema: SchemaId,
	pub name: String,
	pub columns: Vec<ViewColumnDef>,
}
