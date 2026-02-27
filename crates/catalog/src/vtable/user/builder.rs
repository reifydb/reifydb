// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builder for user-defined virtual tables

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		column::{ColumnDef, ColumnIndex},
		id::{ColumnId, NamespaceId},
		vtable::{VTableDef, VTableId},
	},
	value::column::columns::Columns,
};
use reifydb_type::{
	params::Params,
	value::{constraint::TypeConstraint, r#type::Type},
};

use super::UserVTableColumnDef;
use crate::vtable::tables::{UserVTableDataFunction, VTables};

/// Builder for creating user-defined virtual tables.
///
/// # Example
///
/// ```ignore
/// use reifydb_catalog::vtable::UserVTableBuilder;
/// use reifydb_type::value::r#type::Type;
/// use reifydb_core::value::Columns;
///
/// let my_table = UserVTableBuilder::new("my_table")
///     .column("id", Type::Uint64)
///     .column("name", Type::Utf8)
///     .data(|params| {
///         // Return column-oriented data
///         Columns::empty()
///     })
///     .build(NamespaceId(1), VTableId(100));
/// ```
pub struct UserVTableBuilder {
	name: String,
	columns: Vec<UserVTableColumnDef>,
	data_fn: Option<UserVTableDataFunction>,
}

impl UserVTableBuilder {
	/// Create a new user virtual table builder
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			columns: Vec::new(),
			data_fn: None,
		}
	}

	/// Add a column to the virtual table
	pub fn column(mut self, name: impl Into<String>, data_type: Type) -> Self {
		self.columns.push(UserVTableColumnDef::new(name, data_type));
		self
	}

	/// Set the data function that generates table data in columnar format.
	///
	/// The function receives query parameters and should return all data
	/// as `Columns` (column-oriented storage).
	pub fn data<F>(mut self, f: F) -> Self
	where
		F: Fn(&Params) -> Columns + Send + Sync + 'static,
	{
		self.data_fn = Some(Arc::new(f));
		self
	}

	/// Build the virtual table implementation
	///
	/// # Panics
	///
	/// Panics if no data function was provided.
	pub fn build(self, namespace_id: NamespaceId, table_id: VTableId) -> VTables {
		let data_fn = self.data_fn.expect("UserVTableBuilder requires a data function");

		// Build the table definition
		let def = VTableDef {
			id: table_id,
			namespace: namespace_id,
			name: self.name.clone(),
			columns: self
				.columns
				.iter()
				.enumerate()
				.map(|(idx, c)| ColumnDef {
					id: ColumnId(idx as u64),
					name: c.name.clone(),
					constraint: TypeConstraint::unconstrained(c.data_type.clone()),
					properties: Vec::new(),
					index: ColumnIndex(idx as u8),
					auto_increment: false,
					dictionary_id: None,
				})
				.collect(),
		};

		VTables::UserDefined {
			def: Arc::new(def),
			data_fn,
			params: None,
			exhausted: false,
		}
	}
}
