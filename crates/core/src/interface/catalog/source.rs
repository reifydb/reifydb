// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::return_internal_error;
use serde::{Deserialize, Serialize};

use crate::interface::{
	TableDef, TableId, TableVirtualDef, TableVirtualId, ViewDef, ViewId,
};

#[derive(
	Debug,
	Copy,
	Clone,
	PartialOrd,
	PartialEq,
	Ord,
	Eq,
	Hash,
	Serialize,
	Deserialize,
)]
pub enum SourceId {
	Table(TableId),
	View(ViewId),
	TableVirtual(TableVirtualId),
}

impl std::fmt::Display for SourceId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			SourceId::Table(id) => write!(f, "{}", id.0),
			SourceId::View(id) => write!(f, "{}", id.0),
			SourceId::TableVirtual(id) => write!(f, "{}", id.0),
		}
	}
}

impl SourceId {
	pub fn table(id: impl Into<TableId>) -> Self {
		Self::Table(id.into())
	}

	pub fn view(id: impl Into<ViewId>) -> Self {
		Self::View(id.into())
	}

	pub fn table_virtual(id: impl Into<TableVirtualId>) -> Self {
		Self::TableVirtual(id.into())
	}
}

impl From<TableId> for SourceId {
	fn from(id: TableId) -> Self {
		SourceId::Table(id)
	}
}

impl From<ViewId> for SourceId {
	fn from(id: ViewId) -> Self {
		SourceId::View(id)
	}
}

impl From<TableVirtualId> for SourceId {
	fn from(id: TableVirtualId) -> Self {
		SourceId::TableVirtual(id)
	}
}

impl PartialEq<u64> for SourceId {
	fn eq(&self, other: &u64) -> bool {
		match self {
			SourceId::Table(id) => id.0.eq(other),
			SourceId::View(id) => id.0.eq(other),
			SourceId::TableVirtual(id) => id.0.eq(other),
		}
	}
}

impl PartialEq<TableId> for SourceId {
	fn eq(&self, other: &TableId) -> bool {
		match self {
			SourceId::Table(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<ViewId> for SourceId {
	fn eq(&self, other: &ViewId) -> bool {
		match self {
			SourceId::View(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<TableVirtualId> for SourceId {
	fn eq(&self, other: &TableVirtualId) -> bool {
		match self {
			SourceId::TableVirtual(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl From<SourceId> for u64 {
	fn from(source: SourceId) -> u64 {
		source.as_u64()
	}
}

impl SourceId {
	/// Returns the raw u64 value regardless of whether it's a Table, View,
	/// or TableVirtual
	pub fn as_u64(&self) -> u64 {
		match self {
			SourceId::Table(id) => id.0,
			SourceId::View(id) => id.0,
			SourceId::TableVirtual(id) => id.0,
		}
	}

	/// Creates a next source id for range operations (numerically next)
	pub fn next(&self) -> SourceId {
		match self {
			SourceId::Table(table) => SourceId::table(table.0 + 1),
			SourceId::View(view) => SourceId::view(view.0 + 1),
			SourceId::TableVirtual(table_virtual) => {
				SourceId::table_virtual(table_virtual.0 + 1)
			}
		}
	}

	/// Creates a previous source id for range operations (numerically
	/// previous) In descending order encoding, this gives us the next
	/// value in sort order Uses wrapping_sub to handle ID 0 correctly
	/// (wraps to u64::MAX)
	pub fn prev(&self) -> SourceId {
		match self {
			SourceId::Table(table) => {
				SourceId::table(table.0.wrapping_sub(1))
			}
			SourceId::View(view) => {
				SourceId::view(view.0.wrapping_sub(1))
			}
			SourceId::TableVirtual(table_virtual) => {
				SourceId::table_virtual(
					table_virtual.0.wrapping_sub(1),
				)
			}
		}
	}

	pub fn to_table_id(self) -> crate::Result<TableId> {
		if let SourceId::Table(table) = self {
			Ok(table)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SourceId::Table but found {:?}. \
				This indicates a critical catalog inconsistency where a non-table source ID \
				was used in a context that requires a table ID.",
				self
			)
		}
	}

	pub fn to_view_id(self) -> crate::Result<ViewId> {
		if let SourceId::View(view) = self {
			Ok(view)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SourceId::View but found {:?}. \
				This indicates a critical catalog inconsistency where a non-view source ID \
				was used in a context that requires a view ID.",
				self
			)
		}
	}

	pub fn to_table_virtual_id(self) -> crate::Result<TableVirtualId> {
		if let SourceId::TableVirtual(table_virtual) = self {
			Ok(table_virtual)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected SourceId::TableVirtual but found {:?}. \
				This indicates a critical catalog inconsistency where a non-virtual-table source ID \
				was used in a context that requires a virtual table ID.",
				self
			)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SourceDef {
	Table(TableDef),
	View(ViewDef),
	TableVirtual(TableVirtualDef),
}

impl SourceDef {
	pub fn id(&self) -> SourceId {
		match self {
			SourceDef::Table(table) => table.id.into(),
			SourceDef::View(view) => view.id.into(),
			SourceDef::TableVirtual(table_virtual) => {
				table_virtual.id.into()
			}
		}
	}

	pub fn source_type(&self) -> SourceId {
		match self {
			SourceDef::Table(table) => SourceId::Table(table.id),
			SourceDef::View(view) => SourceId::View(view.id),
			SourceDef::TableVirtual(table_virtual) => {
				SourceId::TableVirtual(table_virtual.id)
			}
		}
	}
}
