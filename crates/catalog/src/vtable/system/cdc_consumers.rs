// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes CDC consumer checkpoint information
pub struct SystemCdcConsumers {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemCdcConsumers {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemCdcConsumers {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_cdc_consumers_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemCdcConsumers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// TODO: Implement CDC consumer state retrieval using the new transaction API
		let columns = vec![
			Column {
				name: Fragment::internal("consumer_id"),
				data: ColumnData::utf8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("checkpoint"),
				data: ColumnData::uint8_with_capacity(0),
			},
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
