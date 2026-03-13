// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::ColumnDef,
	id::{NamespaceId, TableId},
	key::PrimaryKeyDef,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
	pub id: TableId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
}

impl TableDef {
	pub fn name(&self) -> &str {
		&self.name
	}
}
