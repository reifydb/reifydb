// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{FlowStatus, TableVirtualDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system flow information
pub struct Flows {
	definition: Arc<TableVirtualDef>,
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

impl<'a> TableVirtual<'a> for Flows {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
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
				name: Fragment::owned_internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::owned_internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: names,
			},
			Column {
				name: Fragment::owned_internal("status"),
				data: statuses,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
