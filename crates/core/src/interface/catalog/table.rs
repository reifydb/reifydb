// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
