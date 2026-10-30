// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::factory},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemWindowedViewGroups {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemWindowedViewGroups {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemWindowedViewGroups {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_windowed_view_groups_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemWindowedViewGroups {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let groups = CatalogStore::list_windowed_view_groups_all(txn)?;
		let n = groups.len();
		let mut ids = Vec::with_capacity(n);
		let mut namespace_ids = Vec::with_capacity(n);
		let mut names = Vec::with_capacity(n);
		let mut flow_ids = Vec::with_capacity(n);

		for group in groups {
			ids.push(group.id.0);
			namespace_ids.push(group.namespace.0);
			names.push(group.name);
			flow_ids.push(group.flow.0);
		}

		let columns = vec![
			factory::uint8("id", ids),
			factory::uint8("namespace_id", namespace_ids),
			factory::utf8("name", names),
			factory::uint8("flow_id", flow_ids),
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
