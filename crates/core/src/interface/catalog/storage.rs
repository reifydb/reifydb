// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	id::{QueueId, RingBufferId, SeriesId, TableId, ViewId},
	object::ObjectId,
};

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum StorageId {
	Table(TableId),
	View(ViewId),
	RingBuffer(RingBufferId),
	Series(SeriesId),
	Queue(QueueId),
}

impl fmt::Display for StorageId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.as_u64())
	}
}

impl StorageId {
	pub fn table(id: impl Into<TableId>) -> Self {
		Self::Table(id.into())
	}

	pub fn view(id: impl Into<ViewId>) -> Self {
		Self::View(id.into())
	}

	pub fn ringbuffer(id: impl Into<RingBufferId>) -> Self {
		Self::RingBuffer(id.into())
	}

	pub fn series(id: impl Into<SeriesId>) -> Self {
		Self::Series(id.into())
	}

	pub fn queue(id: impl Into<QueueId>) -> Self {
		Self::Queue(id.into())
	}

	pub fn type_tag(&self) -> u8 {
		ObjectId::from(*self).type_tag()
	}

	pub fn from_type_tag(tag: u8, id: u64) -> Option<Self> {
		Self::from_object(ObjectId::from_type_tag(tag, id)?)
	}

	pub fn from_object(object: ObjectId) -> Option<Self> {
		match object {
			ObjectId::Table(id) => Some(Self::Table(id)),
			ObjectId::View(id) => Some(Self::View(id)),
			ObjectId::RingBuffer(id) => Some(Self::RingBuffer(id)),
			ObjectId::Series(id) => Some(Self::Series(id)),
			ObjectId::Queue(id) => Some(Self::Queue(id)),
			_ => None,
		}
	}

	pub fn as_u64(&self) -> u64 {
		match self {
			StorageId::Table(id) => id.0,
			StorageId::View(id) => id.0,
			StorageId::RingBuffer(id) => id.0,
			StorageId::Series(id) => id.0,
			StorageId::Queue(id) => id.0,
		}
	}
}

impl From<StorageId> for ObjectId {
	fn from(storage: StorageId) -> Self {
		match storage {
			StorageId::Table(id) => ObjectId::Table(id),
			StorageId::View(id) => ObjectId::View(id),
			StorageId::RingBuffer(id) => ObjectId::RingBuffer(id),
			StorageId::Series(id) => ObjectId::Series(id),
			StorageId::Queue(id) => ObjectId::Queue(id),
		}
	}
}

impl From<TableId> for StorageId {
	fn from(id: TableId) -> Self {
		StorageId::Table(id)
	}
}

impl From<ViewId> for StorageId {
	fn from(id: ViewId) -> Self {
		StorageId::View(id)
	}
}

impl From<RingBufferId> for StorageId {
	fn from(id: RingBufferId) -> Self {
		StorageId::RingBuffer(id)
	}
}

impl From<SeriesId> for StorageId {
	fn from(id: SeriesId) -> Self {
		StorageId::Series(id)
	}
}

impl From<QueueId> for StorageId {
	fn from(id: QueueId) -> Self {
		StorageId::Queue(id)
	}
}

impl PartialEq<TableId> for StorageId {
	fn eq(&self, other: &TableId) -> bool {
		matches!(self, StorageId::Table(id) if id.0 == other.0)
	}
}

impl PartialEq<ViewId> for StorageId {
	fn eq(&self, other: &ViewId) -> bool {
		matches!(self, StorageId::View(id) if id.0 == other.0)
	}
}

impl PartialEq<RingBufferId> for StorageId {
	fn eq(&self, other: &RingBufferId) -> bool {
		matches!(self, StorageId::RingBuffer(id) if id.0 == other.0)
	}
}

impl PartialEq<SeriesId> for StorageId {
	fn eq(&self, other: &SeriesId) -> bool {
		matches!(self, StorageId::Series(id) if id.0 == other.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_type_tags_agree_with_the_wide_object_tags() {
		// `StorageId` has no tag table of its own; giving it one lets the two drift so a key decodes
		// as a different kind with the same id. The loop also covers the arm the compiler cannot:
		// `from_type_tag` narrows through a `_ => None`, so a new variant silently stops decoding.
		for storage in [
			StorageId::Table(TableId(7)),
			StorageId::View(ViewId(7)),
			StorageId::RingBuffer(RingBufferId(7)),
			StorageId::Series(SeriesId(7)),
			StorageId::Queue(QueueId(7)),
		] {
			assert_eq!(storage.type_tag(), ObjectId::from(storage).type_tag());
			assert_eq!(StorageId::from_type_tag(storage.type_tag(), storage.as_u64()), Some(storage));
		}
	}

	#[test]
	fn the_kinds_without_rows_do_not_decode_into_a_storage_id() {
		// A rowless kind must never narrow, otherwise its tag decodes as a table of the same numeric id.
		for object in [ObjectId::vtable(7), ObjectId::dictionary(7)] {
			assert_eq!(StorageId::from_type_tag(object.type_tag(), object.as_u64()), None);
		}
	}

	#[test]
	fn a_view_object_narrows_to_a_view_storage_id() {
		// A view owns its rows, so narrowing must keep the kind and never fall back to `None`.
		assert_eq!(StorageId::from_object(ObjectId::view(7)), Some(StorageId::View(ViewId(7))));
		assert_eq!(StorageId::from(ViewId(7)), StorageId::View(ViewId(7)));
		assert!(StorageId::View(ViewId(7)) == ViewId(7));
		assert!(StorageId::Table(TableId(7)) != ViewId(7));
	}

	#[test]
	fn widening_to_an_object_preserves_the_kind_and_the_id() {
		// Every encoder writes the widened form, so a variant mapped to the wrong `ObjectId` arm
		// writes the wrong tag byte and orphans the row.
		assert_eq!(ObjectId::from(StorageId::Table(TableId(1))), ObjectId::Table(TableId(1)));
		assert_eq!(ObjectId::from(StorageId::View(ViewId(5))), ObjectId::View(ViewId(5)));
		assert_eq!(
			ObjectId::from(StorageId::RingBuffer(RingBufferId(2))),
			ObjectId::RingBuffer(RingBufferId(2))
		);
		assert_eq!(ObjectId::from(StorageId::Series(SeriesId(3))), ObjectId::Series(SeriesId(3)));
		assert_eq!(ObjectId::from(StorageId::Queue(QueueId(4))), ObjectId::Queue(QueueId(4)));
	}
}
