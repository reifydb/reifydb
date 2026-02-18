// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes field information for all registered schemas
pub struct SchemaFields {
	pub(crate) definition: Arc<VTableDef>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SchemaFields {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_schema_fields_table_def().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl VTable for SchemaFields {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let schemas = self.catalog.schema.list_all();

		let total_fields: usize = schemas.iter().map(|s| s.field_count()).sum();

		let mut fingerprints = ColumnData::uint8_with_capacity(total_fields);
		let mut field_indices = ColumnData::uint2_with_capacity(total_fields);
		let mut names = ColumnData::utf8_with_capacity(total_fields);
		let mut types = ColumnData::uint1_with_capacity(total_fields);
		let mut constraint_types = ColumnData::uint1_with_capacity(total_fields);
		let mut constraint_p1s = ColumnData::uint4_with_capacity(total_fields);
		let mut constraint_p2s = ColumnData::uint4_with_capacity(total_fields);
		let mut offsets = ColumnData::uint4_with_capacity(total_fields);
		let mut sizes = ColumnData::uint4_with_capacity(total_fields);
		let mut aligns = ColumnData::uint1_with_capacity(total_fields);

		for schema in schemas {
			let fingerprint = *schema.fingerprint();

			for (idx, field) in schema.fields().iter().enumerate() {
				let ffi = field.constraint.to_ffi();

				fingerprints.push(fingerprint);
				field_indices.push(idx as u16);
				names.push(field.name.as_str());
				types.push(ffi.base_type);
				constraint_types.push(ffi.constraint_type);
				constraint_p1s.push(ffi.constraint_param1);
				constraint_p2s.push(ffi.constraint_param2);
				offsets.push(field.offset);
				sizes.push(field.size);
				aligns.push(field.align);
			}
		}

		let columns = vec![
			Column {
				name: Fragment::internal("fingerprint"),
				data: fingerprints,
			},
			Column {
				name: Fragment::internal("field_index"),
				data: field_indices,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("type"),
				data: types,
			},
			Column {
				name: Fragment::internal("constraint_type"),
				data: constraint_types,
			},
			Column {
				name: Fragment::internal("constraint_p1"),
				data: constraint_p1s,
			},
			Column {
				name: Fragment::internal("constraint_p2"),
				data: constraint_p2s,
			},
			Column {
				name: Fragment::internal("offset"),
				data: offsets,
			},
			Column {
				name: Fragment::internal("size"),
				data: sizes,
			},
			Column {
				name: Fragment::internal("align"),
				data: aligns,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
