// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use reifydb_value::{Result, value::dictionary::DictionaryId};
use serde::{Deserialize, Serialize};

use crate::{
	interface::catalog::{
		id::{RingBufferId, SeriesId, TableId, ViewId},
		table::Table,
		view::View,
		vtable::{VTable, VTableId},
	},
	return_internal_error,
};

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectId {
	Table(TableId),
	View(ViewId),
	TableVirtual(VTableId),
	RingBuffer(RingBufferId),
	Dictionary(DictionaryId),
	Series(SeriesId),
}

impl fmt::Display for ObjectId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ObjectId::Table(id) => write!(f, "{}", id.0),
			ObjectId::View(id) => write!(f, "{}", id.0),
			ObjectId::TableVirtual(id) => write!(f, "{}", id.0),
			ObjectId::RingBuffer(id) => write!(f, "{}", id.0),
			ObjectId::Dictionary(id) => write!(f, "{}", id.0),
			ObjectId::Series(id) => write!(f, "{}", id.0),
		}
	}
}

impl ObjectId {
	pub fn table(id: impl Into<TableId>) -> Self {
		Self::Table(id.into())
	}

	pub fn view(id: impl Into<ViewId>) -> Self {
		Self::View(id.into())
	}

	pub fn vtable(id: impl Into<VTableId>) -> Self {
		Self::TableVirtual(id.into())
	}

	pub fn ringbuffer(id: impl Into<RingBufferId>) -> Self {
		Self::RingBuffer(id.into())
	}

	pub fn dictionary(id: impl Into<DictionaryId>) -> Self {
		Self::Dictionary(id.into())
	}

	pub fn series(id: impl Into<SeriesId>) -> Self {
		Self::Series(id.into())
	}

	#[inline]
	pub fn to_u64(self) -> u64 {
		match self {
			ObjectId::Table(id) => id.to_u64(),
			ObjectId::View(id) => id.to_u64(),
			ObjectId::TableVirtual(id) => id.to_u64(),
			ObjectId::RingBuffer(id) => id.to_u64(),
			ObjectId::Dictionary(id) => id.to_u64(),
			ObjectId::Series(id) => id.to_u64(),
		}
	}
}

impl From<TableId> for ObjectId {
	fn from(id: TableId) -> Self {
		ObjectId::Table(id)
	}
}

impl From<ViewId> for ObjectId {
	fn from(id: ViewId) -> Self {
		ObjectId::View(id)
	}
}

impl From<VTableId> for ObjectId {
	fn from(id: VTableId) -> Self {
		ObjectId::TableVirtual(id)
	}
}

impl From<RingBufferId> for ObjectId {
	fn from(id: RingBufferId) -> Self {
		ObjectId::RingBuffer(id)
	}
}

impl From<DictionaryId> for ObjectId {
	fn from(id: DictionaryId) -> Self {
		ObjectId::Dictionary(id)
	}
}

impl From<SeriesId> for ObjectId {
	fn from(id: SeriesId) -> Self {
		ObjectId::Series(id)
	}
}

impl PartialEq<u64> for ObjectId {
	fn eq(&self, other: &u64) -> bool {
		match self {
			ObjectId::Table(id) => id.0.eq(other),
			ObjectId::View(id) => id.0.eq(other),
			ObjectId::TableVirtual(id) => id.0.eq(other),
			ObjectId::RingBuffer(id) => id.0.eq(other),
			ObjectId::Dictionary(id) => id.0.eq(other),
			ObjectId::Series(id) => id.0.eq(other),
		}
	}
}

impl PartialEq<TableId> for ObjectId {
	fn eq(&self, other: &TableId) -> bool {
		match self {
			ObjectId::Table(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<ViewId> for ObjectId {
	fn eq(&self, other: &ViewId) -> bool {
		match self {
			ObjectId::View(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<VTableId> for ObjectId {
	fn eq(&self, other: &VTableId) -> bool {
		match self {
			ObjectId::TableVirtual(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<RingBufferId> for ObjectId {
	fn eq(&self, other: &RingBufferId) -> bool {
		match self {
			ObjectId::RingBuffer(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<DictionaryId> for ObjectId {
	fn eq(&self, other: &DictionaryId) -> bool {
		match self {
			ObjectId::Dictionary(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<SeriesId> for ObjectId {
	fn eq(&self, other: &SeriesId) -> bool {
		match self {
			ObjectId::Series(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl From<ObjectId> for u64 {
	fn from(object: ObjectId) -> u64 {
		object.as_u64()
	}
}

impl ObjectId {
	pub fn type_tag(&self) -> u8 {
		match self {
			ObjectId::Table(_) => 0x01,
			ObjectId::View(_) => 0x02,
			ObjectId::TableVirtual(_) => 0x03,
			ObjectId::RingBuffer(_) => 0x04,
			ObjectId::Dictionary(_) => 0x05,
			ObjectId::Series(_) => 0x06,
		}
	}

	pub fn from_type_tag(tag: u8, id: u64) -> Option<Self> {
		Some(match tag {
			0x01 => ObjectId::Table(TableId(id)),
			0x02 => ObjectId::View(ViewId(id)),
			0x03 => ObjectId::TableVirtual(VTableId(id)),
			0x04 => ObjectId::RingBuffer(RingBufferId(id)),
			0x05 => ObjectId::Dictionary(DictionaryId(id)),
			0x06 => ObjectId::Series(SeriesId(id)),
			_ => return None,
		})
	}

	pub fn as_u64(&self) -> u64 {
		match self {
			ObjectId::Table(id) => id.0,
			ObjectId::View(id) => id.0,
			ObjectId::TableVirtual(id) => id.0,
			ObjectId::RingBuffer(id) => id.0,
			ObjectId::Dictionary(id) => id.0,
			ObjectId::Series(id) => id.0,
		}
	}

	pub fn next(&self) -> ObjectId {
		match self {
			ObjectId::Table(table) => ObjectId::table(table.0 + 1),
			ObjectId::View(view) => ObjectId::view(view.0 + 1),
			ObjectId::TableVirtual(vtable) => ObjectId::vtable(vtable.0 + 1),
			ObjectId::RingBuffer(ringbuffer) => ObjectId::ringbuffer(ringbuffer.0 + 1),
			ObjectId::Dictionary(dictionary) => ObjectId::dictionary(dictionary.0 + 1),
			ObjectId::Series(series) => ObjectId::series(series.0 + 1),
		}
	}

	pub fn prev(&self) -> ObjectId {
		match self {
			ObjectId::Table(table) => ObjectId::table(table.0.wrapping_sub(1)),
			ObjectId::View(view) => ObjectId::view(view.0.wrapping_sub(1)),
			ObjectId::TableVirtual(vtable) => ObjectId::vtable(vtable.0.wrapping_sub(1)),
			ObjectId::RingBuffer(ringbuffer) => ObjectId::ringbuffer(ringbuffer.0.wrapping_sub(1)),
			ObjectId::Dictionary(dictionary) => ObjectId::dictionary(dictionary.0.wrapping_sub(1)),
			ObjectId::Series(series) => ObjectId::series(series.0.wrapping_sub(1)),
		}
	}

	pub fn to_table_id(self) -> Result<TableId> {
		if let ObjectId::Table(table) = self {
			Ok(table)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ObjectId::Table but found {:?}. \
				This indicates a critical catalog inconsistency where a non-table object ID \
				was used in a context that requires a table ID.",
				self
			)
		}
	}

	pub fn to_view_id(self) -> Result<ViewId> {
		if let ObjectId::View(view) = self {
			Ok(view)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ObjectId::View but found {:?}. \
				This indicates a critical catalog inconsistency where a non-view object ID \
				was used in a context that requires a view ID.",
				self
			)
		}
	}

	pub fn to_vtable_id(self) -> Result<VTableId> {
		if let ObjectId::TableVirtual(vtable) = self {
			Ok(vtable)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ObjectId::TableVirtual but found {:?}. \
				This indicates a critical catalog inconsistency where a non-virtual-table object ID \
				was used in a context that requires a virtual table ID.",
				self
			)
		}
	}

	pub fn to_ringbuffer_id(self) -> Result<RingBufferId> {
		if let ObjectId::RingBuffer(ringbuffer) = self {
			Ok(ringbuffer)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ObjectId::RingBuffer but found {:?}. \
				This indicates a critical catalog inconsistency where a non-ring-buffer object ID \
				was used in a context that requires a ring buffer ID.",
				self
			)
		}
	}

	pub fn to_dictionary_id(self) -> Result<DictionaryId> {
		if let ObjectId::Dictionary(dictionary) = self {
			Ok(dictionary)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ObjectId::Dictionary but found {:?}. \
				This indicates a critical catalog inconsistency where a non-dictionary object ID \
				was used in a context that requires a dictionary ID.",
				self
			)
		}
	}

	pub fn to_series_id(self) -> Result<SeriesId> {
		if let ObjectId::Series(series) = self {
			Ok(series)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ObjectId::Series but found {:?}. \
				This indicates a critical catalog inconsistency where a non-series object ID \
				was used in a context that requires a series ID.",
				self
			)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Object {
	Table(Table),
	View(View),
	TableVirtual(VTable),
}

impl Object {
	pub fn id(&self) -> ObjectId {
		match self {
			Object::Table(table) => table.id.into(),
			Object::View(view) => view.id().into(),
			Object::TableVirtual(vtable) => vtable.id.into(),
		}
	}

	pub fn object_type(&self) -> ObjectId {
		match self {
			Object::Table(table) => ObjectId::Table(table.id),
			Object::View(view) => ObjectId::View(view.id()),
			Object::TableVirtual(vtable) => ObjectId::TableVirtual(vtable.id),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The tag is written into the catalog and row-settings keyspaces, so these bytes are on-disk
	/// layout: changing one silently orphans every row already stored under the old tag. Pinned
	/// literally rather than derived, so a reordering of the enum cannot move them. 0x05 used to
	/// belong to the retired `Flow` variant and was reclaimed once no writer could emit it.
	#[test]
	fn the_type_tags_are_contiguous_and_pinned_to_their_on_disk_bytes() {
		assert_eq!(ObjectId::Table(TableId(1)).type_tag(), 0x01);
		assert_eq!(ObjectId::View(ViewId(1)).type_tag(), 0x02);
		assert_eq!(ObjectId::TableVirtual(VTableId(1)).type_tag(), 0x03);
		assert_eq!(ObjectId::RingBuffer(RingBufferId(1)).type_tag(), 0x04);
		assert_eq!(ObjectId::Dictionary(DictionaryId(1)).type_tag(), 0x05);
		assert_eq!(ObjectId::Series(SeriesId(1)).type_tag(), 0x06);
	}

	/// Accepting an out-of-range tag would let a corrupt or truncated key decode into a neighbouring
	/// kind instead of erroring, which is how a wrong-kind lookup turns into a silent miss rather
	/// than a fault. 0x07 is called out specifically because it was Series' tag before the range was
	/// made contiguous, so a key left over from an older database must fault rather than decode.
	#[test]
	fn a_tag_outside_the_assigned_range_is_rejected_rather_than_mapped_to_a_neighbour() {
		assert_eq!(ObjectId::from_type_tag(0x00, 42), None);
		assert_eq!(ObjectId::from_type_tag(0x07, 42), None);
		assert_eq!(ObjectId::from_type_tag(0x08, 42), None);
		assert_eq!(ObjectId::from_type_tag(0xff, 42), None);
	}

	/// Encoders write `type_tag` and decoders read `from_type_tag`; if the two tables ever drift
	/// apart again, a key would decode as a different object kind carrying the same numeric id.
	#[test]
	fn every_variant_survives_a_tag_round_trip_as_the_same_kind() {
		let objects = [
			ObjectId::Table(TableId(7)),
			ObjectId::View(ViewId(7)),
			ObjectId::TableVirtual(VTableId(7)),
			ObjectId::RingBuffer(RingBufferId(7)),
			ObjectId::Dictionary(DictionaryId(7)),
			ObjectId::Series(SeriesId(7)),
		];

		for object in objects {
			assert_eq!(ObjectId::from_type_tag(object.type_tag(), object.as_u64()), Some(object));
		}
	}
}
