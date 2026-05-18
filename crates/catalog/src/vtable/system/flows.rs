// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{flow::FlowStatus, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemFlows {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemFlows {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemFlows {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_flows_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemFlows {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let flows = CatalogStore::list_flows_all(txn)?;

		let mut ids = ColumnBuffer::uint8_with_capacity(flows.len());
		let mut namespaces = ColumnBuffer::uint8_with_capacity(flows.len());
		let mut names = ColumnBuffer::utf8_with_capacity(flows.len());
		let mut statuses = ColumnBuffer::utf8_with_capacity(flows.len());

		for flow in flows {
			ids.push(flow.id.0);
			namespaces.push(flow.namespace.0);
			names.push(flow.name.as_str());

			let status_str = match flow.status {
				FlowStatus::Active => "Active",
				FlowStatus::Paused => "Paused",
				FlowStatus::Failed => "Failed",
			};
			statuses.push(status_str);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("status"), statuses),
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
