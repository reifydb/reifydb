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

pub struct SystemShapes {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemShapes {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_shapes_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemShapes {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let shapes = self.catalog.cache.list_row_shapes();

		let mut fingerprints = ColumnBuffer::uint8_with_capacity(shapes.len());
		let mut field_counts = ColumnBuffer::uint2_with_capacity(shapes.len());

		for shape in shapes {
			fingerprints.push(*shape.fingerprint());
			field_counts.push(shape.field_count() as u16);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("fingerprint"), fingerprints),
			ColumnWithName::new(Fragment::internal("field_count"), field_counts),
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
