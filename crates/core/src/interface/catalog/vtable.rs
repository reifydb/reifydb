// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{column::Column, id::NamespaceId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VTableId(pub u64);

impl From<u64> for VTableId {
	fn from(id: u64) -> Self {
		VTableId(id)
	}
}

impl From<VTableId> for u64 {
	fn from(id: VTableId) -> u64 {
		id.0
	}
}

impl VTableId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VTable {
	pub id: VTableId,

	pub namespace: NamespaceId,

	pub name: String,

	pub columns: Vec<Column>,
}
