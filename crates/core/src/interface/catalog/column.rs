// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_type::value::{constraint::TypeConstraint, dictionary::DictionaryId};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{id::ColumnId, policy::ColumnPolicy};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
	pub id: ColumnId,
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicy>,
	pub index: ColumnIndex,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnIndex(pub u8);

impl Deref for ColumnIndex {
	type Target = u8;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u8> for ColumnIndex {
	fn eq(&self, other: &u8) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnIndex> for u8 {
	fn from(value: ColumnIndex) -> Self {
		value.0
	}
}
