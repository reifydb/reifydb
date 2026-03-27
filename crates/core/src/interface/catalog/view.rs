// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::sumtype::SumTypeId;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::Column,
	id::{NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
	key::PrimaryKey,
	schema::SchemaId,
	series::SeriesKey,
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
pub struct TableView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub underlying: TableId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub underlying: RingBufferId,
	pub capacity: u64,
	pub propagate_evictions: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub underlying: SeriesId,
	pub key: SeriesKey,
	pub tag: Option<SumTypeId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum View {
	Table(TableView),
	RingBuffer(RingBufferView),
	Series(SeriesView),
}

impl View {
	pub fn id(&self) -> ViewId {
		match self {
			View::Table(t) => t.id,
			View::RingBuffer(rb) => rb.id,
			View::Series(s) => s.id,
		}
	}

	pub fn namespace(&self) -> NamespaceId {
		match self {
			View::Table(t) => t.namespace,
			View::RingBuffer(rb) => rb.namespace,
			View::Series(s) => s.namespace,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			View::Table(t) => &t.name,
			View::RingBuffer(rb) => &rb.name,
			View::Series(s) => &s.name,
		}
	}

	pub fn kind(&self) -> ViewKind {
		match self {
			View::Table(t) => t.kind,
			View::RingBuffer(rb) => rb.kind,
			View::Series(s) => s.kind,
		}
	}

	pub fn columns(&self) -> &[Column] {
		match self {
			View::Table(t) => &t.columns,
			View::RingBuffer(rb) => &rb.columns,
			View::Series(s) => &s.columns,
		}
	}

	pub fn columns_mut(&mut self) -> &mut Vec<Column> {
		match self {
			View::Table(t) => &mut t.columns,
			View::RingBuffer(rb) => &mut rb.columns,
			View::Series(s) => &mut s.columns,
		}
	}

	pub fn primary_key(&self) -> Option<&PrimaryKey> {
		match self {
			View::Table(t) => t.primary_key.as_ref(),
			View::RingBuffer(rb) => rb.primary_key.as_ref(),
			View::Series(s) => s.primary_key.as_ref(),
		}
	}

	pub fn storage_kind(&self) -> ViewStorageKind {
		match self {
			View::Table(_) => ViewStorageKind::Table,
			View::RingBuffer(_) => ViewStorageKind::RingBuffer,
			View::Series(_) => ViewStorageKind::Series,
		}
	}

	/// Returns the SchemaId of the underlying backing primitive.
	///
	/// All view data is stored under the underlying primitive's key space,
	/// not under `SchemaId::View`.
	pub fn underlying_id(&self) -> SchemaId {
		match self {
			View::Table(t) => SchemaId::Table(t.underlying),
			View::RingBuffer(rb) => SchemaId::RingBuffer(rb.underlying),
			View::Series(s) => SchemaId::Series(s.underlying),
		}
	}

	pub fn set_name(&mut self, new_name: String) {
		match self {
			View::Table(t) => t.name = new_name,
			View::RingBuffer(rb) => rb.name = new_name,
			View::Series(s) => s.name = new_name,
		}
	}

	pub fn set_namespace(&mut self, new_namespace: NamespaceId) {
		match self {
			View::Table(t) => t.namespace = new_namespace,
			View::RingBuffer(rb) => rb.namespace = new_namespace,
			View::Series(s) => s.namespace = new_namespace,
		}
	}
}
