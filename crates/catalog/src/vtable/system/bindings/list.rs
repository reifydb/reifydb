// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use super::common_vtable_columns;
use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemBindings {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemBindings {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemBindings {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_bindings_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemBindings {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let bindings = CatalogStore::list_bindings_all(txn)?;

		let mut formats = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());
		let mut kinds = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());

		for b in &bindings {
			formats.push(b.format.as_str());
			kinds.push(b.protocol.protocol_str());
		}

		let mut columns = common_vtable_columns(&bindings);
		columns.extend(vec![formats.finish("format"), kinds.finish("kind")]);

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
