// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{constraint::TypeConstraint, sumtype::SumTypeId};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::NamespaceId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SumTypeDef {
	pub id: SumTypeId,
	pub namespace: NamespaceId,
	pub name: String,
	pub variants: Vec<VariantDef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VariantDef {
	pub tag: u8,
	pub name: String,
	pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDef {
	pub name: String,
	pub field_type: TypeConstraint,
}
