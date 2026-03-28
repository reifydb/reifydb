// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use reifydb_type::{Result, value::dictionary::DictionaryId};
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

/// SchemaId represents identifiers for catalog primitives that use u64-based IDs.
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum SchemaId {
	Table(TableId),
	View(ViewId),
	TableVirtual(VTableId),
	RingBuffer(RingBufferId),
	Dictionary(DictionaryId),
	Series(SeriesId),
}

impl fmt::Display for SchemaId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SchemaId::Table(id) => write!(f, "{}", id.0),
			SchemaId::View(id) => write!(f, "{}", id.0),
			SchemaId::TableVirtual(id) => write!(f, "{}", id.0),
			SchemaId::RingBuffer(id) => write!(f, "{}", id.0),
			SchemaId::Dictionary(id) => write!(f, "{}", id.0),
			SchemaId::Series(id) => write!(f, "{}", id.0),
		}
	}
}

impl SchemaId {
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

	/// Get the inner u64 value from the ID variant.
	#[inline]
	pub fn to_u64(self) -> u64 {
		match self {
			SchemaId::Table(id) => id.to_u64(),
			SchemaId::View(id) => id.to_u64(),
			SchemaId::TableVirtual(id) => id.to_u64(),
			SchemaId::RingBuffer(id) => id.to_u64(),
			SchemaId::Dictionary(id) => id.to_u64(),
			SchemaId::Series(id) => id.to_u64(),
		}
	}
}

impl From<TableId> for SchemaId {
	fn from(id: TableId) -> Self {
		SchemaId::Table(id)
	}
}

impl From<ViewId> for SchemaId {
	fn from(id: ViewId) -> Self {
		SchemaId::View(id)
	}
}

impl From<VTableId> for SchemaId {
	fn from(id: VTableId) -> Self {
		SchemaId::TableVirtual(id)
	}
}

impl From<RingBufferId> for SchemaId {
	fn from(id: RingBufferId) -> Self {
		SchemaId::RingBuffer(id)
	}
}

impl From<DictionaryId> for SchemaId {
	fn from(id: DictionaryId) -> Self {
		SchemaId::Dictionary(id)
	}
}

impl From<SeriesId> for SchemaId {
	fn from(id: SeriesId) -> Self {
		SchemaId::Series(id)
	}
}

impl PartialEq<u64> for SchemaId {
	fn eq(&self, other: &u64) -> bool {
		match self {
			SchemaId::Table(id) => id.0.eq(other),
			SchemaId::View(id) => id.0.eq(other),
			SchemaId::TableVirtual(id) => id.0.eq(other),
			SchemaId::RingBuffer(id) => id.0.eq(other),
			SchemaId::Dictionary(id) => id.0.eq(other),
			SchemaId::Series(id) => id.0.eq(other),
		}
	}
}

impl PartialEq<TableId> for SchemaId {
	fn eq(&self, other: &TableId) -> bool {
		match self {
			SchemaId::Table(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<ViewId> for SchemaId {
	fn eq(&self, other: &ViewId) -> bool {
		match self {
			SchemaId::View(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<VTableId> for SchemaId {
	fn eq(&self, other: &VTableId) -> bool {
		match self {
			SchemaId::TableVirtual(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<RingBufferId> for SchemaId {
	fn eq(&self, other: &RingBufferId) -> bool {
		match self {
			SchemaId::RingBuffer(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<DictionaryId> for SchemaId {
	fn eq(&self, other: &DictionaryId) -> bool {
		match self {
			SchemaId::Dictionary(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<SeriesId> for SchemaId {
	fn eq(&self, other: &SeriesId) -> bool {
		match self {
			SchemaId::Series(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl From<SchemaId> for u64 {
	fn from(object: SchemaId) -> u64 {
		object.as_u64()
	}
}

impl SchemaId {
	/// Returns the type discriminant as a u8 value
	pub fn to_type_u8(&self) -> u8 {
		match self {
			SchemaId::Table(_) => 1,
			SchemaId::View(_) => 2,
			SchemaId::TableVirtual(_) => 3,
			SchemaId::RingBuffer(_) => 4,
			SchemaId::Dictionary(_) => 5,
			SchemaId::Series(_) => 6,
		}
	}

	/// Returns the raw u64 value regardless of the object type
	pub fn as_u64(&self) -> u64 {
		match self {
			SchemaId::Table(id) => id.0,
			SchemaId::View(id) => id.0,
			SchemaId::TableVirtual(id) => id.0,
			SchemaId::RingBuffer(id) => id.0,
			SchemaId::Dictionary(id) => id.0,
			SchemaId::Series(id) => id.0,
		}
	}

	/// Creates a next object id for range operations (numerically next)
	pub fn next(&self) -> SchemaId {
		match self {
			SchemaId::Table(table) => SchemaId::table(table.0 + 1),
			SchemaId::View(view) => SchemaId::view(view.0 + 1),
			SchemaId::TableVirtual(vtable) => SchemaId::vtable(vtable.0 + 1),
			SchemaId::RingBuffer(ringbuffer) => SchemaId::ringbuffer(ringbuffer.0 + 1),
			SchemaId::Dictionary(dictionary) => SchemaId::dictionary(dictionary.0 + 1),
			SchemaId::Series(series) => SchemaId::series(series.0 + 1),
		}
	}

	/// Creates a previous object id for range operations (numerically
	/// previous) In descending order encoding, this gives us the next
	/// value in sort order Uses wrapping_sub to handle ID 0 correctly
	/// (wraps to u64::MAX)
	pub fn prev(&self) -> SchemaId {
		match self {
			SchemaId::Table(table) => SchemaId::table(table.0.wrapping_sub(1)),
			SchemaId::View(view) => SchemaId::view(view.0.wrapping_sub(1)),
			SchemaId::TableVirtual(vtable) => SchemaId::vtable(vtable.0.wrapping_sub(1)),
			SchemaId::RingBuffer(ringbuffer) => SchemaId::ringbuffer(ringbuffer.0.wrapping_sub(1)),
			SchemaId::Dictionary(dictionary) => SchemaId::dictionary(dictionary.0.wrapping_sub(1)),
			SchemaId::Series(series) => SchemaId::series(series.0.wrapping_sub(1)),
		}
	}

	pub fn to_table_id(self) -> Result<TableId> {
		if let SchemaId::Table(table) = self {
			Ok(table)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SchemaId::Table but found {:?}. \
				This indicates a critical catalog inconsistency where a non-table object ID \
				was used in a context that requires a table ID.",
				self
			)
		}
	}

	pub fn to_view_id(self) -> Result<ViewId> {
		if let SchemaId::View(view) = self {
			Ok(view)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SchemaId::View but found {:?}. \
				This indicates a critical catalog inconsistency where a non-view object ID \
				was used in a context that requires a view ID.",
				self
			)
		}
	}

	pub fn to_vtable_id(self) -> Result<VTableId> {
		if let SchemaId::TableVirtual(vtable) = self {
			Ok(vtable)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SchemaId::TableVirtual but found {:?}. \
				This indicates a critical catalog inconsistency where a non-virtual-table object ID \
				was used in a context that requires a virtual table ID.",
				self
			)
		}
	}

	pub fn to_ringbuffer_id(self) -> Result<RingBufferId> {
		if let SchemaId::RingBuffer(ringbuffer) = self {
			Ok(ringbuffer)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SchemaId::RingBuffer but found {:?}. \
				This indicates a critical catalog inconsistency where a non-ring-buffer object ID \
				was used in a context that requires a ring buffer ID.",
				self
			)
		}
	}

	pub fn to_dictionary_id(self) -> Result<DictionaryId> {
		if let SchemaId::Dictionary(dictionary) = self {
			Ok(dictionary)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SchemaId::Dictionary but found {:?}. \
				This indicates a critical catalog inconsistency where a non-dictionary object ID \
				was used in a context that requires a dictionary ID.",
				self
			)
		}
	}

	pub fn to_series_id(self) -> Result<SeriesId> {
		if let SchemaId::Series(series) = self {
			Ok(series)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SchemaId::Series but found {:?}. \
				This indicates a critical catalog inconsistency where a non-series object ID \
				was used in a context that requires a series ID.",
				self
			)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Schema {
	Table(Table),
	View(View),
	TableVirtual(VTable),
}

impl Schema {
	pub fn id(&self) -> SchemaId {
		match self {
			Schema::Table(table) => table.id.into(),
			Schema::View(view) => view.id().into(),
			Schema::TableVirtual(vtable) => vtable.id.into(),
		}
	}

	pub fn schema_type(&self) -> SchemaId {
		match self {
			Schema::Table(table) => SchemaId::Table(table.id),
			Schema::View(view) => SchemaId::View(view.id()),
			Schema::TableVirtual(vtable) => SchemaId::TableVirtual(vtable.id),
		}
	}
}
