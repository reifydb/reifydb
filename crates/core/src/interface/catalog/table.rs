// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{
	interface::catalog::{
		column::ColumnDef,
		id::{NamespaceId, TableId},
		key::PrimaryKeyDef,
	},
	value::encoded::named::EncodedValuesNamedLayout,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
	pub id: TableId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
}

impl From<&TableDef> for EncodedValuesNamedLayout {
	fn from(value: &TableDef) -> Self {
		value.columns.as_slice().into()
	}
}
