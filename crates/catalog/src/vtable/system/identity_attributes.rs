// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

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

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("value_type"), value_types.finish()),
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
