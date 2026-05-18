// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::Column,
	id::{NamespaceId, TableId},
	key::PrimaryKey,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
	pub id: TableId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub underlying: bool,
}

impl Table {
	pub fn name(&self) -> &str {
		&self.name
	}
}
