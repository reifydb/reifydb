// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{
	interface::{ColumnDef, NamespaceId, PrimaryKeyDef, TableId},
	value::encoded::EncodedValuesNamedLayout,
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
