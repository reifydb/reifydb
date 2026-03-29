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

/// ShapeId represents identifiers for catalog primitives that use u64-based IDs.
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum ShapeId {
	Table(TableId),
	View(ViewId),
	TableVirtual(VTableId),
	RingBuffer(RingBufferId),
	Dictionary(DictionaryId),
	Series(SeriesId),
}

impl fmt::Display for ShapeId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ShapeId::Table(id) => write!(f, "{}", id.0),
			ShapeId::View(id) => write!(f, "{}", id.0),
			ShapeId::TableVirtual(id) => write!(f, "{}", id.0),
			ShapeId::RingBuffer(id) => write!(f, "{}", id.0),
			ShapeId::Dictionary(id) => write!(f, "{}", id.0),
			ShapeId::Series(id) => write!(f, "{}", id.0),
		}
	}
}

impl ShapeId {
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
			ShapeId::Table(id) => id.to_u64(),
			ShapeId::View(id) => id.to_u64(),
			ShapeId::TableVirtual(id) => id.to_u64(),
			ShapeId::RingBuffer(id) => id.to_u64(),
			ShapeId::Dictionary(id) => id.to_u64(),
			ShapeId::Series(id) => id.to_u64(),
		}
	}
}

impl From<TableId> for ShapeId {
	fn from(id: TableId) -> Self {
		ShapeId::Table(id)
	}
}

impl From<ViewId> for ShapeId {
	fn from(id: ViewId) -> Self {
		ShapeId::View(id)
	}
}

impl From<VTableId> for ShapeId {
	fn from(id: VTableId) -> Self {
		ShapeId::TableVirtual(id)
	}
}

impl From<RingBufferId> for ShapeId {
	fn from(id: RingBufferId) -> Self {
		ShapeId::RingBuffer(id)
	}
}

impl From<DictionaryId> for ShapeId {
	fn from(id: DictionaryId) -> Self {
		ShapeId::Dictionary(id)
	}
}

impl From<SeriesId> for ShapeId {
	fn from(id: SeriesId) -> Self {
		ShapeId::Series(id)
	}
}

impl PartialEq<u64> for ShapeId {
	fn eq(&self, other: &u64) -> bool {
		match self {
			ShapeId::Table(id) => id.0.eq(other),
			ShapeId::View(id) => id.0.eq(other),
			ShapeId::TableVirtual(id) => id.0.eq(other),
			ShapeId::RingBuffer(id) => id.0.eq(other),
			ShapeId::Dictionary(id) => id.0.eq(other),
			ShapeId::Series(id) => id.0.eq(other),
		}
	}
}

impl PartialEq<TableId> for ShapeId {
	fn eq(&self, other: &TableId) -> bool {
		match self {
			ShapeId::Table(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<ViewId> for ShapeId {
	fn eq(&self, other: &ViewId) -> bool {
		match self {
			ShapeId::View(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<VTableId> for ShapeId {
	fn eq(&self, other: &VTableId) -> bool {
		match self {
			ShapeId::TableVirtual(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<RingBufferId> for ShapeId {
	fn eq(&self, other: &RingBufferId) -> bool {
		match self {
			ShapeId::RingBuffer(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<DictionaryId> for ShapeId {
	fn eq(&self, other: &DictionaryId) -> bool {
		match self {
			ShapeId::Dictionary(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<SeriesId> for ShapeId {
	fn eq(&self, other: &SeriesId) -> bool {
		match self {
			ShapeId::Series(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl From<ShapeId> for u64 {
	fn from(shape: ShapeId) -> u64 {
		shape.as_u64()
	}
}

impl ShapeId {
	/// Returns the type discriminant as a u8 value
	pub fn to_type_u8(&self) -> u8 {
		match self {
			ShapeId::Table(_) => 1,
			ShapeId::View(_) => 2,
			ShapeId::TableVirtual(_) => 3,
			ShapeId::RingBuffer(_) => 4,
			ShapeId::Dictionary(_) => 5,
			ShapeId::Series(_) => 6,
		}
	}

	/// Returns the raw u64 value regardless of the object type
	pub fn as_u64(&self) -> u64 {
		match self {
			ShapeId::Table(id) => id.0,
			ShapeId::View(id) => id.0,
			ShapeId::TableVirtual(id) => id.0,
			ShapeId::RingBuffer(id) => id.0,
			ShapeId::Dictionary(id) => id.0,
			ShapeId::Series(id) => id.0,
		}
	}

	/// Creates a next object id for range operations (numerically next)
	pub fn next(&self) -> ShapeId {
		match self {
			ShapeId::Table(table) => ShapeId::table(table.0 + 1),
			ShapeId::View(view) => ShapeId::view(view.0 + 1),
			ShapeId::TableVirtual(vtable) => ShapeId::vtable(vtable.0 + 1),
			ShapeId::RingBuffer(ringbuffer) => ShapeId::ringbuffer(ringbuffer.0 + 1),
			ShapeId::Dictionary(dictionary) => ShapeId::dictionary(dictionary.0 + 1),
			ShapeId::Series(series) => ShapeId::series(series.0 + 1),
		}
	}

	/// Creates a previous object id for range operations (numerically
	/// previous) In descending order encoding, this gives us the next
	/// value in sort order Uses wrapping_sub to handle ID 0 correctly
	/// (wraps to u64::MAX)
	pub fn prev(&self) -> ShapeId {
		match self {
			ShapeId::Table(table) => ShapeId::table(table.0.wrapping_sub(1)),
			ShapeId::View(view) => ShapeId::view(view.0.wrapping_sub(1)),
			ShapeId::TableVirtual(vtable) => ShapeId::vtable(vtable.0.wrapping_sub(1)),
			ShapeId::RingBuffer(ringbuffer) => ShapeId::ringbuffer(ringbuffer.0.wrapping_sub(1)),
			ShapeId::Dictionary(dictionary) => ShapeId::dictionary(dictionary.0.wrapping_sub(1)),
			ShapeId::Series(series) => ShapeId::series(series.0.wrapping_sub(1)),
		}
	}

	pub fn to_table_id(self) -> Result<TableId> {
		if let ShapeId::Table(table) = self {
			Ok(table)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ShapeId::Table but found {:?}. \
				This indicates a critical catalog inconsistency where a non-table object ID \
				was used in a context that requires a table ID.",
				self
			)
		}
	}

	pub fn to_view_id(self) -> Result<ViewId> {
		if let ShapeId::View(view) = self {
			Ok(view)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ShapeId::View but found {:?}. \
				This indicates a critical catalog inconsistency where a non-view object ID \
				was used in a context that requires a view ID.",
				self
			)
		}
	}

	pub fn to_vtable_id(self) -> Result<VTableId> {
		if let ShapeId::TableVirtual(vtable) = self {
			Ok(vtable)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ShapeId::TableVirtual but found {:?}. \
				This indicates a critical catalog inconsistency where a non-virtual-table object ID \
				was used in a context that requires a virtual table ID.",
				self
			)
		}
	}

	pub fn to_ringbuffer_id(self) -> Result<RingBufferId> {
		if let ShapeId::RingBuffer(ringbuffer) = self {
			Ok(ringbuffer)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ShapeId::RingBuffer but found {:?}. \
				This indicates a critical catalog inconsistency where a non-ring-buffer object ID \
				was used in a context that requires a ring buffer ID.",
				self
			)
		}
	}

	pub fn to_dictionary_id(self) -> Result<DictionaryId> {
		if let ShapeId::Dictionary(dictionary) = self {
			Ok(dictionary)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ShapeId::Dictionary but found {:?}. \
				This indicates a critical catalog inconsistency where a non-dictionary object ID \
				was used in a context that requires a dictionary ID.",
				self
			)
		}
	}

	pub fn to_series_id(self) -> Result<SeriesId> {
		if let ShapeId::Series(series) = self {
			Ok(series)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected ShapeId::Series but found {:?}. \
				This indicates a critical catalog inconsistency where a non-series object ID \
				was used in a context that requires a series ID.",
				self
			)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Shape {
	Table(Table),
	View(View),
	TableVirtual(VTable),
}

impl Shape {
	pub fn id(&self) -> ShapeId {
		match self {
			Shape::Table(table) => table.id.into(),
			Shape::View(view) => view.id().into(),
			Shape::TableVirtual(vtable) => vtable.id.into(),
		}
	}

	pub fn shape_type(&self) -> ShapeId {
		match self {
			Shape::Table(table) => ShapeId::Table(table.id),
			Shape::View(view) => ShapeId::View(view.id()),
			Shape::TableVirtual(vtable) => ShapeId::TableVirtual(vtable.id),
		}
	}
}
