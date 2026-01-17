// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes registered schema information
pub struct Schemas {
	pub(crate) definition: Arc<VTableDef>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl Schemas {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_schemas_table_def().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl<T: IntoStandardTransaction> VTable<T> for Schemas {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let schemas = self.catalog.schema.list_all();

		let mut fingerprints = ColumnData::uint8_with_capacity(schemas.len());
		let mut field_counts = ColumnData::uint2_with_capacity(schemas.len());

		for schema in schemas {
			fingerprints.push(*schema.fingerprint());
			field_counts.push(schema.field_count() as u16);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("fingerprint"),
				data: fingerprints,
			},
			Column {
				name: Fragment::internal("field_count"),
				data: field_counts,
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
