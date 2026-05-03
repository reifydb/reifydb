// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		column::{Column, ColumnIndex},
		id::{ColumnId, NamespaceId},
		vtable::{VTable, VTableId},
	},
	value::column::columns::Columns,
};
use reifydb_type::{
	params::Params,
	value::{constraint::TypeConstraint, r#type::Type},
};

use super::UserVTableColumn;
use crate::vtable::tables::{UserVTableDataFunction, VTables};

pub struct UserVTableBuilder {
	name: String,
	columns: Vec<UserVTableColumn>,
	data_fn: Option<UserVTableDataFunction>,
}

impl UserVTableBuilder {
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			columns: Vec::new(),
			data_fn: None,
		}
	}

	pub fn column(mut self, name: impl Into<String>, data_type: Type) -> Self {
		self.columns.push(UserVTableColumn::new(name, data_type));
		self
	}

	pub fn data<F>(mut self, f: F) -> Self
	where
		F: Fn(&Params) -> Columns + Send + Sync + 'static,
	{
		self.data_fn = Some(Arc::new(f));
		self
	}

	pub fn build(self, namespace_id: NamespaceId, table_id: VTableId) -> VTables {
		let data_fn = self.data_fn.expect("UserVTableBuilder requires a data function");

		let def = VTable {
			id: table_id,
			namespace: namespace_id,
			name: self.name.clone(),
			columns: self
				.columns
				.iter()
				.enumerate()
				.map(|(idx, c)| Column {
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
			vtable: Arc::new(def),
			data_fn,
			params: None,
			exhausted: false,
		}
	}
}
