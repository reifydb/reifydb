// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{flow::FlowStatus, vtable::VTable},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

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

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, flows.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, flows.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, flows.len());
		let mut statuses = ColumnBuilder::with_capacity(ValueType::Utf8, flows.len());

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
			ids.finish("id"),
			namespaces.finish("namespace_id"),
			names.finish("name"),
			statuses.finish("status"),
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
