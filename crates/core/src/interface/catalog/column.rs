// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use reifydb_type::TypeConstraint;
use serde::{Deserialize, Serialize};

use crate::{
	interface::{ColumnId, ColumnPolicy},
	value::encoded::EncodedValuesNamedLayout,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
	pub id: ColumnId,
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicy>,
	pub index: ColumnIndex,
	pub auto_increment: bool,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnIndex(pub u16);

impl Deref for ColumnIndex {
	type Target = u16;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u16> for ColumnIndex {
	fn eq(&self, other: &u16) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnIndex> for u16 {
	fn from(value: ColumnIndex) -> Self {
		value.0
	}
}

impl From<&[ColumnDef]> for EncodedValuesNamedLayout {
	fn from(value: &[ColumnDef]) -> Self {
		EncodedValuesNamedLayout::new(value.iter().map(|col| (col.name.clone(), col.constraint.get_type())))
	}
}
