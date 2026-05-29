// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{constraint::TypeConstraint, sumtype::SumTypeId};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::NamespaceId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum SumTypeKind {
	#[default]
	Enum = 0,
	Event = 1,
	Tag = 2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SumType {
	pub id: SumTypeId,
	pub namespace: NamespaceId,
	pub name: String,
	pub variants: Vec<Variant>,
	#[serde(default)]
	pub kind: SumTypeKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Variant {
	pub tag: u8,
	pub name: String,
	pub fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
	pub name: String,
	pub field_type: TypeConstraint,
}
