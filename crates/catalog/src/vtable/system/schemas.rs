// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes registered schema information
pub struct SystemSchemas {
	pub(crate) definition: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemSchemas {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_schemas_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemSchemas {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
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

	fn definition(&self) -> &VTable {
		&self.definition
	}
}
