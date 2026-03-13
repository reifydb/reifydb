// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::sumtype::SumTypeId;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::ColumnDef,
	id::{NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
	key::PrimaryKeyDef,
	primitive::PrimitiveId,
	series::TimestampPrecision,
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ViewKind {
	Deferred,
	Transactional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ViewStorageKind {
	Table = 1,
	RingBuffer = 2,
	Series = 3,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableViewDef {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
	pub underlying: TableId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferViewDef {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
	pub underlying: RingBufferId,
	pub capacity: u64,
	pub propagate_evictions: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesViewDef {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Option<PrimaryKeyDef>,
	pub underlying: SeriesId,
	pub timestamp_column: Option<String>,
	pub precision: TimestampPrecision,
	pub tag: Option<SumTypeId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ViewDef {
	Table(TableViewDef),
	RingBuffer(RingBufferViewDef),
	Series(SeriesViewDef),
}

impl ViewDef {
	pub fn id(&self) -> ViewId {
		match self {
			ViewDef::Table(t) => t.id,
			ViewDef::RingBuffer(rb) => rb.id,
			ViewDef::Series(s) => s.id,
		}
	}

	pub fn namespace(&self) -> NamespaceId {
		match self {
			ViewDef::Table(t) => t.namespace,
			ViewDef::RingBuffer(rb) => rb.namespace,
			ViewDef::Series(s) => s.namespace,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			ViewDef::Table(t) => &t.name,
			ViewDef::RingBuffer(rb) => &rb.name,
			ViewDef::Series(s) => &s.name,
		}
	}

	pub fn kind(&self) -> ViewKind {
		match self {
			ViewDef::Table(t) => t.kind,
			ViewDef::RingBuffer(rb) => rb.kind,
			ViewDef::Series(s) => s.kind,
		}
	}

	pub fn columns(&self) -> &[ColumnDef] {
		match self {
			ViewDef::Table(t) => &t.columns,
			ViewDef::RingBuffer(rb) => &rb.columns,
			ViewDef::Series(s) => &s.columns,
		}
	}

	pub fn columns_mut(&mut self) -> &mut Vec<ColumnDef> {
		match self {
			ViewDef::Table(t) => &mut t.columns,
			ViewDef::RingBuffer(rb) => &mut rb.columns,
			ViewDef::Series(s) => &mut s.columns,
		}
	}

	pub fn primary_key(&self) -> Option<&PrimaryKeyDef> {
		match self {
			ViewDef::Table(t) => t.primary_key.as_ref(),
			ViewDef::RingBuffer(rb) => rb.primary_key.as_ref(),
			ViewDef::Series(s) => s.primary_key.as_ref(),
		}
	}

	pub fn storage_kind(&self) -> ViewStorageKind {
		match self {
			ViewDef::Table(_) => ViewStorageKind::Table,
			ViewDef::RingBuffer(_) => ViewStorageKind::RingBuffer,
			ViewDef::Series(_) => ViewStorageKind::Series,
		}
	}

	/// Returns the PrimitiveId of the underlying backing primitive.
	///
	/// All view data is stored under the underlying primitive's key space,
	/// not under `PrimitiveId::View`.
	pub fn underlying_id(&self) -> PrimitiveId {
		match self {
			ViewDef::Table(t) => PrimitiveId::Table(t.underlying),
			ViewDef::RingBuffer(rb) => PrimitiveId::RingBuffer(rb.underlying),
			ViewDef::Series(s) => PrimitiveId::Series(s.underlying),
		}
	}

	pub fn set_name(&mut self, new_name: String) {
		match self {
			ViewDef::Table(t) => t.name = new_name,
			ViewDef::RingBuffer(rb) => rb.name = new_name,
			ViewDef::Series(s) => s.name = new_name,
		}
	}

	pub fn set_namespace(&mut self, new_namespace: NamespaceId) {
		match self {
			ViewDef::Table(t) => t.namespace = new_namespace,
			ViewDef::RingBuffer(rb) => rb.namespace = new_namespace,
			ViewDef::Series(s) => s.namespace = new_namespace,
		}
	}
}
