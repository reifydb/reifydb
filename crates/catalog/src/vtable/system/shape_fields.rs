// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemShapeFields {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemShapeFields {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_shape_fields_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemShapeFields {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let shapes = self.catalog.materialized.list_row_shapes();

		let total_fields: usize = shapes.iter().map(|s| s.field_count()).sum();

		let mut fingerprints = ColumnBuffer::uint8_with_capacity(total_fields);
		let mut field_indices = ColumnBuffer::uint2_with_capacity(total_fields);
		let mut names = ColumnBuffer::utf8_with_capacity(total_fields);
		let mut types = ColumnBuffer::uint1_with_capacity(total_fields);
		let mut constraint_types = ColumnBuffer::uint1_with_capacity(total_fields);
		let mut constraint_p1s = ColumnBuffer::uint4_with_capacity(total_fields);
		let mut constraint_p2s = ColumnBuffer::uint4_with_capacity(total_fields);
		let mut offsets = ColumnBuffer::uint4_with_capacity(total_fields);
		let mut sizes = ColumnBuffer::uint4_with_capacity(total_fields);
		let mut aligns = ColumnBuffer::uint1_with_capacity(total_fields);

		for shape in shapes {
			let fingerprint = *shape.fingerprint();

			for (idx, field) in shape.fields().iter().enumerate() {
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
			ColumnWithName::new(Fragment::internal("fingerprint"), fingerprints),
			ColumnWithName::new(Fragment::internal("field_index"), field_indices),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("type"), types),
			ColumnWithName::new(Fragment::internal("constraint_type"), constraint_types),
			ColumnWithName::new(Fragment::internal("constraint_p1"), constraint_p1s),
			ColumnWithName::new(Fragment::internal("constraint_p2"), constraint_p2s),
			ColumnWithName::new(Fragment::internal("offset"), offsets),
			ColumnWithName::new(Fragment::internal("size"), sizes),
			ColumnWithName::new(Fragment::internal("align"), aligns),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
