// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowFamily;
use reifydb_value::value::sumtype::SumTypeId;
use serde::{Deserialize, Serialize};

use crate::{
	interface::catalog::{
		column::{Column, ColumnIndex},
		id::{NamespaceId, QueueId, ViewId},
		key::PrimaryKey,
		series::SeriesKey,
		storage::StorageId,
	},
	sort::SortDirection,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewSortKey {
	pub column: ColumnIndex,
	pub direction: SortDirection,
}

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
	Queue = 4,
}

impl ViewStorageKind {
	pub fn row_family(self) -> RowFamily {
		match self {
			Self::Table => RowFamily::Table,
			Self::RingBuffer => RowFamily::RingBuffer,
			Self::Series => RowFamily::Series,
			Self::Queue => RowFamily::Queue,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub sort: Vec<ViewSortKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub capacity: u64,
	pub sort: Vec<ViewSortKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub key: SeriesKey,
	pub tag: Option<SumTypeId>,
	pub sort: Vec<ViewSortKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueView {
	pub id: ViewId,
	pub namespace: NamespaceId,
	pub name: String,
	pub kind: ViewKind,
	pub columns: Vec<Column>,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub queue: QueueId,
	pub sort: Vec<ViewSortKey>,
}

impl QueueView {
	pub const CHANGE_COLUMN: &'static str = "change";
	pub const CHANGE_INSERT: &'static str = "insert";
	pub const CHANGE_UPDATE: &'static str = "update";
	pub const CHANGE_REMOVE: &'static str = "remove";
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum View {
	Table(TableView),
	RingBuffer(RingBufferView),
	Series(SeriesView),
	Queue(QueueView),
}

impl View {
	pub fn id(&self) -> ViewId {
		match self {
			View::Table(t) => t.id,
			View::RingBuffer(rb) => rb.id,
			View::Series(s) => s.id,
			View::Queue(q) => q.id,
		}
	}

	pub fn namespace(&self) -> NamespaceId {
		match self {
			View::Table(t) => t.namespace,
			View::RingBuffer(rb) => rb.namespace,
			View::Series(s) => s.namespace,
			View::Queue(q) => q.namespace,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			View::Table(t) => &t.name,
			View::RingBuffer(rb) => &rb.name,
			View::Series(s) => &s.name,
			View::Queue(q) => &q.name,
		}
	}

	pub fn kind(&self) -> ViewKind {
		match self {
			View::Table(t) => t.kind,
			View::RingBuffer(rb) => rb.kind,
			View::Series(s) => s.kind,
			View::Queue(q) => q.kind,
		}
	}

	pub fn columns(&self) -> &[Column] {
		match self {
			View::Table(t) => &t.columns,
			View::RingBuffer(rb) => &rb.columns,
			View::Series(s) => &s.columns,
			View::Queue(q) => &q.columns,
		}
	}

	pub fn columns_mut(&mut self) -> &mut Vec<Column> {
		match self {
			View::Table(t) => &mut t.columns,
			View::RingBuffer(rb) => &mut rb.columns,
			View::Series(s) => &mut s.columns,
			View::Queue(q) => &mut q.columns,
		}
	}

	pub fn primary_key(&self) -> Option<&PrimaryKey> {
		match self {
			View::Table(t) => t.primary_key.as_ref(),
			View::RingBuffer(rb) => rb.primary_key.as_ref(),
			View::Series(s) => s.primary_key.as_ref(),
			View::Queue(q) => q.primary_key.as_ref(),
		}
	}

	pub fn sort(&self) -> &[ViewSortKey] {
		match self {
			View::Table(t) => &t.sort,
			View::RingBuffer(rb) => &rb.sort,
			View::Series(s) => &s.sort,
			View::Queue(q) => &q.sort,
		}
	}

	pub fn storage_kind(&self) -> ViewStorageKind {
		match self {
			View::Table(_) => ViewStorageKind::Table,
			View::RingBuffer(_) => ViewStorageKind::RingBuffer,
			View::Series(_) => ViewStorageKind::Series,
			View::Queue(_) => ViewStorageKind::Queue,
		}
	}

	pub fn storage_id(&self) -> StorageId {
		match self {
			View::Queue(q) => StorageId::Queue(q.queue),
			other => StorageId::View(other.id()),
		}
	}

	pub fn partition_by(&self) -> &[String] {
		match self {
			View::Table(t) => &t.partition_by,
			View::RingBuffer(rb) => &rb.partition_by,
			View::Series(s) => &s.partition_by,
			View::Queue(q) => &q.partition_by,
		}
	}

	pub fn set_name(&mut self, new_name: String) {
		match self {
			View::Table(t) => t.name = new_name,
			View::RingBuffer(rb) => rb.name = new_name,
			View::Series(s) => s.name = new_name,
			View::Queue(q) => q.name = new_name,
		}
	}

	pub fn set_namespace(&mut self, new_namespace: NamespaceId) {
		match self {
			View::Table(t) => t.namespace = new_namespace,
			View::RingBuffer(rb) => rb.namespace = new_namespace,
			View::Series(s) => s.namespace = new_namespace,
			View::Queue(q) => q.namespace = new_namespace,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_queue_view_stores_its_rows_under_its_queue_and_every_other_view_under_itself() {
		// Claim, reap and retention read the queue's row keys, so a queue view scanning its own id reads
		// nothing.
		let queue_view = View::Queue(QueueView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "q".to_string(),
			kind: ViewKind::Deferred,
			columns: vec![],
			primary_key: None,
			partition_by: vec![],
			queue: QueueId(2),
			sort: vec![],
		});
		assert_eq!(queue_view.storage_id(), StorageId::Queue(QueueId(2)));
		assert_eq!(queue_view.id(), ViewId(1), "the view keeps its own identity in the catalog");
		assert_eq!(queue_view.storage_kind(), ViewStorageKind::Queue);
		assert_eq!(ViewStorageKind::Queue.row_family(), RowFamily::Queue);

		let table_view = View::Table(TableView {
			id: ViewId(3),
			namespace: NamespaceId(1),
			name: "t".to_string(),
			kind: ViewKind::Deferred,
			columns: vec![],
			primary_key: None,
			partition_by: vec![],
			sort: vec![],
		});
		assert_eq!(table_view.storage_id(), StorageId::View(ViewId(3)));
	}
}
