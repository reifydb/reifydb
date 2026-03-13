// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::ColumnDef,
	id::{NamespaceId, ViewId},
	key::PrimaryKeyDef,
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

impl ViewDef {
	pub fn id(&self) -> ViewId {
		self.id
	}

	pub fn namespace(&self) -> NamespaceId {
		self.namespace
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn kind(&self) -> ViewKind {
		self.kind
	}

	pub fn columns(&self) -> &[ColumnDef] {
		&self.columns
	}

	pub fn columns_mut(&mut self) -> &mut Vec<ColumnDef> {
		&mut self.columns
	}

	pub fn primary_key(&self) -> Option<&PrimaryKeyDef> {
		self.primary_key.as_ref()
	}
}
