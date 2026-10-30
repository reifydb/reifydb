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

pub struct SystemWindowedViewGroupMembers {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemWindowedViewGroupMembers {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemWindowedViewGroupMembers {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_windowed_view_group_members_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemWindowedViewGroupMembers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let groups = CatalogStore::list_windowed_view_groups_all(txn)?;
		let mut group_ids = Vec::new();
		let mut view_ids = Vec::new();

		for group in groups {
			for member in group.members {
				group_ids.push(group.id.0);
				view_ids.push(member.0);
			}
		}

		let columns = vec![factory::uint8("group_id", group_ids), factory::uint8("view_id", view_ids)];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
