// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::constraint::encode_type_constraint;
use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemRowShapeFields {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemRowShapeFields {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_row_shape_fields_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemRowShapeFields {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let shapes = self.catalog.cache.list_row_shapes();

		let total_fields: usize = shapes.iter().map(|s| s.field_count()).sum();

		let mut fingerprints = ColumnBuilder::with_capacity(ValueType::Uint8, total_fields);
		let mut field_indices = ColumnBuilder::with_capacity(ValueType::Uint2, total_fields);
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, total_fields);
		let mut types = ColumnBuilder::with_capacity(ValueType::Uint1, total_fields);
		let mut constraint_types = ColumnBuilder::with_capacity(ValueType::Uint1, total_fields);
		let mut constraint_p1s = ColumnBuilder::with_capacity(ValueType::Uint4, total_fields);
		let mut constraint_p2s = ColumnBuilder::with_capacity(ValueType::Uint4, total_fields);
		let mut offsets = ColumnBuilder::with_capacity(ValueType::Uint4, total_fields);
		let mut sizes = ColumnBuilder::with_capacity(ValueType::Uint4, total_fields);

		for shape in shapes {
			let fingerprint = *shape.fingerprint();

			for (idx, field) in shape.fields().iter().enumerate() {
				let extern_c = encode_type_constraint(&field.constraint)
					.expect("constraint exceeds tag capacity");

				fingerprints.push(fingerprint);
				field_indices.push(idx as u16);
				names.push(field.name.as_str());
				types.push(extern_c.base_type);
				constraint_types.push(extern_c.constraint_type);
				constraint_p1s.push(extern_c.constraint_param1);
				constraint_p2s.push(extern_c.constraint_param2);
				offsets.push(field.offset);
				sizes.push(field.size);
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("fingerprint"), fingerprints.finish()),
			ColumnWithName::new(Fragment::internal("field_index"), field_indices.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("type"), types.finish()),
			ColumnWithName::new(Fragment::internal("constraint_type"), constraint_types.finish()),
			ColumnWithName::new(Fragment::internal("constraint_p1"), constraint_p1s.finish()),
			ColumnWithName::new(Fragment::internal("constraint_p2"), constraint_p2s.finish()),
			ColumnWithName::new(Fragment::internal("offset"), offsets.finish()),
			ColumnWithName::new(Fragment::internal("size"), sizes.finish()),
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
