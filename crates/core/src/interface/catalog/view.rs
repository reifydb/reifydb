// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{
	interface::{ColumnDef, NamespaceId, PrimaryKeyDef, ViewId},
	value::encoded::EncodedValuesNamedLayout,
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ViewKind {
	Deferred,
	Transactional,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewDef {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
}

impl From<&ViewDef> for EncodedValuesNamedLayout {
	fn from(value: &ViewDef) -> Self {
		value.columns.as_slice().into()
	}
}
