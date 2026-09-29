// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemIdentityAttributes {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemIdentityAttributes {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemIdentityAttributes {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_identity_attributes_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemIdentityAttributes {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let attributes = CatalogStore::list_all_identity_attributes(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, attributes.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, attributes.len());
		let mut value_types = ColumnBuilder::with_capacity(ValueType::Utf8, attributes.len());

		for a in attributes {
			ids.push(a.id);
			names.push(a.name.as_str());
			value_types.push(a.value_type.to_string());
		}

		let columns = vec![ids.finish("id"), names.finish("name"), value_types.finish("value_type")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
