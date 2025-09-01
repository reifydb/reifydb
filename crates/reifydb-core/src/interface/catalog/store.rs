// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{
	interface::{
		TableDef, TableId, TableVirtualDef, TableVirtualId, ViewDef,
		ViewId,
	},
	return_internal_error,
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
pub enum StoreId {
	Table(TableId),
	View(ViewId),
	TableVirtual(TableVirtualId),
}

impl std::fmt::Display for StoreId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			StoreId::Table(id) => write!(f, "{}", id.0),
			StoreId::View(id) => write!(f, "{}", id.0),
			StoreId::TableVirtual(id) => write!(f, "{}", id.0),
		}
	}
}

impl StoreId {
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

impl From<TableId> for StoreId {
	fn from(id: TableId) -> Self {
		StoreId::Table(id)
	}
}

impl From<ViewId> for StoreId {
	fn from(id: ViewId) -> Self {
		StoreId::View(id)
	}
}

impl From<TableVirtualId> for StoreId {
	fn from(id: TableVirtualId) -> Self {
		StoreId::TableVirtual(id)
	}
}

impl PartialEq<u64> for StoreId {
	fn eq(&self, other: &u64) -> bool {
		match self {
			StoreId::Table(id) => id.0.eq(other),
			StoreId::View(id) => id.0.eq(other),
			StoreId::TableVirtual(id) => id.0.eq(other),
		}
	}
}

impl PartialEq<TableId> for StoreId {
	fn eq(&self, other: &TableId) -> bool {
		match self {
			StoreId::Table(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<ViewId> for StoreId {
	fn eq(&self, other: &ViewId) -> bool {
		match self {
			StoreId::View(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl PartialEq<TableVirtualId> for StoreId {
	fn eq(&self, other: &TableVirtualId) -> bool {
		match self {
			StoreId::TableVirtual(id) => id.0 == other.0,
			_ => false,
		}
	}
}

impl From<StoreId> for u64 {
	fn from(store: StoreId) -> u64 {
		store.as_u64()
	}
}

impl StoreId {
	/// Returns the raw u64 value regardless of whether it's a Table, View,
	/// or TableVirtual
	pub fn as_u64(&self) -> u64 {
		match self {
			StoreId::Table(id) => id.0,
			StoreId::View(id) => id.0,
			StoreId::TableVirtual(id) => id.0,
		}
	}

	/// Creates a next store id for range operations (numerically next)
	pub fn next(&self) -> StoreId {
		match self {
			StoreId::Table(table) => StoreId::table(table.0 + 1),
			StoreId::View(view) => StoreId::view(view.0 + 1),
			StoreId::TableVirtual(table_virtual) => {
				StoreId::table_virtual(table_virtual.0 + 1)
			}
		}
	}

	/// Creates a previous store id for range operations (numerically
	/// previous) In descending order encoding, this gives us the next
	/// value in sort order Uses wrapping_sub to handle ID 0 correctly
	/// (wraps to u64::MAX)
	pub fn prev(&self) -> StoreId {
		match self {
			StoreId::Table(table) => {
				StoreId::table(table.0.wrapping_sub(1))
			}
			StoreId::View(view) => {
				StoreId::view(view.0.wrapping_sub(1))
			}
			StoreId::TableVirtual(table_virtual) => {
				StoreId::table_virtual(
					table_virtual.0.wrapping_sub(1),
				)
			}
		}
	}

	pub fn to_table_id(self) -> crate::Result<TableId> {
		if let StoreId::Table(table) = self {
			Ok(table)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected StoreId::Table but found {:?}. \
				This indicates a critical catalog inconsistency where a non-table store ID \
				was used in a context that requires a table ID.",
				self
			)
		}
	}

	pub fn to_view_id(self) -> crate::Result<ViewId> {
		if let StoreId::View(view) = self {
			Ok(view)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected StoreId::View but found {:?}. \
				This indicates a critical catalog inconsistency where a non-view store ID \
				was used in a context that requires a view ID.",
				self
			)
		}
	}

	pub fn to_table_virtual_id(self) -> crate::Result<TableVirtualId> {
		if let StoreId::TableVirtual(table_virtual) = self {
			Ok(table_virtual)
		} else {
			return_internal_error!(
				"Data inconsistency: Expected StoreId::TableVirtual but found {:?}. \
				This indicates a critical catalog inconsistency where a non-virtual-table store ID \
				was used in a context that requires a virtual table ID.",
				self
			)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StoreDef {
	Table(TableDef),
	View(ViewDef),
	TableVirtual(TableVirtualDef),
}

impl StoreDef {
	pub fn id(&self) -> StoreId {
		match self {
			StoreDef::Table(table) => table.id.into(),
			StoreDef::View(view) => view.id.into(),
			StoreDef::TableVirtual(table_virtual) => {
				table_virtual.id.into()
			}
		}
	}

	pub fn store_type(&self) -> StoreId {
		match self {
			StoreDef::Table(table) => StoreId::Table(table.id),
			StoreDef::View(view) => StoreId::View(view.id),
			StoreDef::TableVirtual(table_virtual) => {
				StoreId::TableVirtual(table_virtual.id)
			}
		}
	}
}
