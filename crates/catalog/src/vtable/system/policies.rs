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
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemPolicies {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemPolicies {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemPolicies {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_policies_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemPolicies {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let policies = CatalogStore::list_all_policies(txn)?;

		let mut ids = ColumnBuffer::uint8_with_capacity(policies.len());
		let mut names = ColumnBuffer::utf8_with_capacity(policies.len());
		let mut target_types = ColumnBuffer::utf8_with_capacity(policies.len());
		let mut target_namespaces = ColumnBuffer::utf8_with_capacity(policies.len());
		let mut target_shapes = ColumnBuffer::utf8_with_capacity(policies.len());
		let mut enabled_flags = ColumnBuffer::bool_with_capacity(policies.len());

		for p in policies {
			ids.push(p.id);
			names.push(p.name.as_deref().unwrap_or(""));
			target_types.push(p.target_type.as_str());
			target_namespaces.push(p.target_namespace.as_deref().unwrap_or(""));
			target_shapes.push(p.target_shape.as_deref().unwrap_or(""));
			enabled_flags.push(p.enabled);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("target_type"), target_types),
			ColumnWithName::new(Fragment::internal("target_namespace"), target_namespaces),
			ColumnWithName::new(Fragment::internal("target_shape"), target_shapes),
			ColumnWithName::new(Fragment::internal("enabled"), enabled_flags),
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
