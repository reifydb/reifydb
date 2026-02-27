// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system policy information
pub struct Policies {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Policies {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_policies_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Policies {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let policies = CatalogStore::list_all_policies(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(policies.len());
		let mut names = ColumnData::utf8_with_capacity(policies.len());
		let mut target_types = ColumnData::utf8_with_capacity(policies.len());
		let mut target_namespaces = ColumnData::utf8_with_capacity(policies.len());
		let mut target_objects = ColumnData::utf8_with_capacity(policies.len());
		let mut enabled_flags = ColumnData::bool_with_capacity(policies.len());

		for p in policies {
			ids.push(p.id);
			names.push(p.name.as_deref().unwrap_or(""));
			target_types.push(p.target_type.as_str());
			target_namespaces.push(p.target_namespace.as_deref().unwrap_or(""));
			target_objects.push(p.target_object.as_deref().unwrap_or(""));
			enabled_flags.push(p.enabled);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("target_type"),
				data: target_types,
			},
			Column {
				name: Fragment::internal("target_namespace"),
				data: target_namespaces,
			},
			Column {
				name: Fragment::internal("target_object"),
				data: target_objects,
			},
			Column {
				name: Fragment::internal("enabled"),
				data: enabled_flags,
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
