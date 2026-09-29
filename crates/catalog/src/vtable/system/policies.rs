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

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, policies.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, policies.len());
		let mut target_types = ColumnBuilder::with_capacity(ValueType::Utf8, policies.len());
		let mut target_namespaces = ColumnBuilder::with_capacity(ValueType::Utf8, policies.len());
		let mut target_objects = ColumnBuilder::with_capacity(ValueType::Utf8, policies.len());
		let mut enabled_flags = ColumnBuilder::with_capacity(ValueType::Boolean, policies.len());

		for p in policies {
			ids.push(p.id);
			names.push(p.name.as_deref().unwrap_or(""));
			target_types.push(p.target_type.as_str());
			target_namespaces.push(p.target_namespace.as_deref().unwrap_or(""));
			target_objects.push(p.target_object.as_deref().unwrap_or(""));
			enabled_flags.push(p.enabled);
		}

		let columns = vec![
			ids.finish("id"),
			names.finish("name"),
			target_types.finish("target_type"),
			target_namespaces.finish("target_namespace"),
			target_objects.finish("target_object"),
			enabled_flags.finish("enabled"),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
