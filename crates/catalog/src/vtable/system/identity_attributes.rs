// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

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

		let mut ids = ColumnBuffer::uint8_with_capacity(attributes.len());
		let mut names = ColumnBuffer::utf8_with_capacity(attributes.len());
		let mut value_types = ColumnBuffer::utf8_with_capacity(attributes.len());

		for a in attributes {
			ids.push(a.id);
			names.push(a.name.as_str());
			value_types.push(a.value_type.to_string());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("value_type"), value_types),
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
