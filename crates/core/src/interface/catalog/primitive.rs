// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::return_internal_error;
use serde::{Deserialize, Serialize};

use crate::interface::{
	DictionaryId, FlowDef, FlowId, RingBufferId, TableDef, TableId, TableVirtualDef, TableVirtualId, ViewDef,
	ViewId,
};

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum PrimitiveId {
	Table(TableId),
	View(ViewId),
	Flow(FlowId),
	TableVirtual(TableVirtualId),
	RingBuffer(RingBufferId),
	Dictionary(DictionaryId),
}

impl std::fmt::Display for PrimitiveId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			PrimitiveId::Table(id) => write!(f, "{}", id.0),
			PrimitiveId::View(id) => write!(f, "{}", id.0),
			PrimitiveId::Flow(id) => write!(f, "{}", id.0),
			PrimitiveId::TableVirtual(id) => write!(f, "{}", id.0),
			PrimitiveId::RingBuffer(id) => write!(f, "{}", id.0),
			PrimitiveId::Dictionary(id) => write!(f, "{}", id.0),
		}
	}
}

impl PrimitiveId {
	pub fn table(id: impl Into<TableId>) -> Self {
		Self::Table(id.into())
	}

	pub fn view(id: impl Into<ViewId>) -> Self {
		Self::View(id.into())
	}

	pub fn flow(id: impl Into<FlowId>) -> Self {
		Self::Flow(id.into())
	}

	pub fn table_virtual(id: impl Into<TableVirtualId>) -> Self {
		Self::TableVirtual(id.into())
	}

	pub fn ringbuffer(id: impl Into<RingBufferId>) -> Self {
		Self::RingBuffer(id.into())
	}

	pub fn dictionary(id: impl Into<DictionaryId>) -> Self {
		Self::Dictionary(id.into())
	}
}

impl From<TableId> for PrimitiveId {
	fn from(id: TableId) -> Self {
		PrimitiveId::Table(id)
	}
}

impl From<ViewId> for PrimitiveId {
	fn from(id: ViewId) -> Self {
		PrimitiveId::View(id)
	}
}

impl From<FlowId> for PrimitiveId {
	fn from(id: FlowId) -> Self {
		PrimitiveId::Flow(id)
	}
}

impl From<TableVirtualId> for PrimitiveId {
	fn from(id: TableVirtualId) -> Self {
		PrimitiveId::TableVirtual(id)
	}
}

impl From<RingBufferId> for PrimitiveId {
	fn from(id: RingBufferId) -> Self {
		PrimitiveId::RingBuffer(id)
	}
}

impl From<DictionaryId> for PrimitiveId {
	fn from(id: DictionaryId) -> Self {
		PrimitiveId::Dictionary(id)
	}
}

impl PartialEq<u64> for PrimitiveId {
	fn eq(&self, other: &u64) -> bool {
		match self {
			PrimitiveId::Table(id) => id.0.eq(other),
			PrimitiveId::View(id) => id.0.eq(other),
			PrimitiveId::Flow(id) => id.0.eq(other),
			PrimitiveId::TableVirtual(id) => id.0.eq(other),
			PrimitiveId::RingBuffer(id) => id.0.eq(other),
			PrimitiveId::Dictionary(id) => id.0.eq(other),
		}
	}
}

impl PartialEq<TableId> for PrimitiveId {
	fn eq(&self, other: &TableId) -> bool {
		match self {
			PrimitiveId::Table(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<ViewId> for PrimitiveId {
	fn eq(&self, other: &ViewId) -> bool {
		match self {
			PrimitiveId::View(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<FlowId> for PrimitiveId {
	fn eq(&self, other: &FlowId) -> bool {
		match self {
			PrimitiveId::Flow(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<TableVirtualId> for PrimitiveId {
	fn eq(&self, other: &TableVirtualId) -> bool {
		match self {
			PrimitiveId::TableVirtual(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<RingBufferId> for PrimitiveId {
	fn eq(&self, other: &RingBufferId) -> bool {
		match self {
			PrimitiveId::RingBuffer(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<DictionaryId> for PrimitiveId {
	fn eq(&self, other: &DictionaryId) -> bool {
		match self {
			PrimitiveId::Dictionary(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl From<PrimitiveId> for u64 {
	fn from(primitive: PrimitiveId) -> u64 {
		primitive.as_u64()
	}
}

impl PrimitiveId {
	/// Returns the type discriminant as a u8 value
	pub fn to_type_u8(&self) -> u8 {
		match self {
			PrimitiveId::Table(_) => 1,
			PrimitiveId::View(_) => 2,
			PrimitiveId::Flow(_) => 3,
			PrimitiveId::TableVirtual(_) => 4,
			PrimitiveId::RingBuffer(_) => 5,
			PrimitiveId::Dictionary(_) => 6,
		}
	}

	/// Returns the raw u64 value regardless of the primitive type
	pub fn as_u64(&self) -> u64 {
		match self {
			PrimitiveId::Table(id) => id.0,
			PrimitiveId::View(id) => id.0,
			PrimitiveId::Flow(id) => id.0,
			PrimitiveId::TableVirtual(id) => id.0,
			PrimitiveId::RingBuffer(id) => id.0,
			PrimitiveId::Dictionary(id) => id.0,
		}
	}

	/// Creates a next primitive id for range operations (numerically next)
	pub fn next(&self) -> PrimitiveId {
		match self {
			PrimitiveId::Table(table) => PrimitiveId::table(table.0 + 1),
			PrimitiveId::View(view) => PrimitiveId::view(view.0 + 1),
			PrimitiveId::Flow(flow) => PrimitiveId::flow(flow.0 + 1),
			PrimitiveId::TableVirtual(table_virtual) => PrimitiveId::table_virtual(table_virtual.0 + 1),
			PrimitiveId::RingBuffer(ringbuffer) => PrimitiveId::ringbuffer(ringbuffer.0 + 1),
			PrimitiveId::Dictionary(dictionary) => PrimitiveId::dictionary(dictionary.0 + 1),
		}
	}

	/// Creates a previous primitive id for range operations (numerically
	/// previous) In descending order encoding, this gives us the next
	/// value in sort order Uses wrapping_sub to handle ID 0 correctly
	/// (wraps to u64::MAX)
	pub fn prev(&self) -> PrimitiveId {
		match self {
			PrimitiveId::Table(table) => PrimitiveId::table(table.0.wrapping_sub(1)),
			PrimitiveId::View(view) => PrimitiveId::view(view.0.wrapping_sub(1)),
			PrimitiveId::Flow(flow) => PrimitiveId::flow(flow.0.wrapping_sub(1)),
			PrimitiveId::TableVirtual(table_virtual) => {
				PrimitiveId::table_virtual(table_virtual.0.wrapping_sub(1))
			}
			PrimitiveId::RingBuffer(ringbuffer) => PrimitiveId::ringbuffer(ringbuffer.0.wrapping_sub(1)),
			PrimitiveId::Dictionary(dictionary) => PrimitiveId::dictionary(dictionary.0.wrapping_sub(1)),
		}
	}

	pub fn to_table_id(self) -> crate::Result<TableId> {
		if let PrimitiveId::Table(table) = self {
			Ok(table)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected PrimitiveId::Table but found {:?}. \
				This indicates a critical catalog inconsistency where a non-table primitive ID \
				was used in a context that requires a table ID.",
				self
			)
		}
	}

	pub fn to_view_id(self) -> crate::Result<ViewId> {
		if let PrimitiveId::View(view) = self {
			Ok(view)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected PrimitiveId::View but found {:?}. \
				This indicates a critical catalog inconsistency where a non-view primitive ID \
				was used in a context that requires a view ID.",
				self
			)
		}
	}

	pub fn to_flow_id(self) -> crate::Result<FlowId> {
		if let PrimitiveId::Flow(flow) = self {
			Ok(flow)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected PrimitiveId::Flow but found {:?}. \
				This indicates a critical catalog inconsistency where a non-flow primitive ID \
				was used in a context that requires a flow ID.",
				self
			)
		}
	}

	pub fn to_table_virtual_id(self) -> crate::Result<TableVirtualId> {
		if let PrimitiveId::TableVirtual(table_virtual) = self {
			Ok(table_virtual)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected PrimitiveId::TableVirtual but found {:?}. \
				This indicates a critical catalog inconsistency where a non-virtual-table primitive ID \
				was used in a context that requires a virtual table ID.",
				self
			)
		}
	}

	pub fn to_ringbuffer_id(self) -> crate::Result<RingBufferId> {
		if let PrimitiveId::RingBuffer(ringbuffer) = self {
			Ok(ringbuffer)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected PrimitiveId::RingBuffer but found {:?}. \
				This indicates a critical catalog inconsistency where a non-ring-buffer primitive ID \
				was used in a context that requires a ring buffer ID.",
				self
			)
		}
	}

	pub fn to_dictionary_id(self) -> crate::Result<DictionaryId> {
		if let PrimitiveId::Dictionary(dictionary) = self {
			Ok(dictionary)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected PrimitiveId::Dictionary but found {:?}. \
				This indicates a critical catalog inconsistency where a non-dictionary primitive ID \
				was used in a context that requires a dictionary ID.",
				self
			)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrimitiveDef {
	Table(TableDef),
	View(ViewDef),
	Flow(FlowDef),
	TableVirtual(TableVirtualDef),
}

impl PrimitiveDef {
	pub fn id(&self) -> PrimitiveId {
		match self {
			PrimitiveDef::Table(table) => table.id.into(),
			PrimitiveDef::View(view) => view.id.into(),
			PrimitiveDef::Flow(flow) => flow.id.into(),
			PrimitiveDef::TableVirtual(table_virtual) => table_virtual.id.into(),
		}
	}

	pub fn primitive_type(&self) -> PrimitiveId {
		match self {
			PrimitiveDef::Table(table) => PrimitiveId::Table(table.id),
			PrimitiveDef::View(view) => PrimitiveId::View(view.id),
			PrimitiveDef::Flow(flow) => PrimitiveId::Flow(flow.id),
			PrimitiveDef::TableVirtual(table_virtual) => PrimitiveId::TableVirtual(table_virtual.id),
		}
	}
}
