// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{flow::FlowStatus, vtable::VTableDef},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system flow information
pub struct Flows {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Flows {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_flows_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<T: AsTransaction> VTable<T> for Flows {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let flows = CatalogStore::list_flows_all(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(flows.len());
		let mut namespaces = ColumnData::uint8_with_capacity(flows.len());
		let mut names = ColumnData::utf8_with_capacity(flows.len());
		let mut statuses = ColumnData::utf8_with_capacity(flows.len());

		for flow in flows {
			ids.push(flow.id.0);
			namespaces.push(flow.namespace.0);
			names.push(flow.name.as_str());

			// Convert FlowStatus enum to string
			let status_str = match flow.status {
				FlowStatus::Active => "Active",
				FlowStatus::Paused => "Paused",
				FlowStatus::Failed => "Failed",
			};
			statuses.push(status_str);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("status"),
				data: statuses,
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
