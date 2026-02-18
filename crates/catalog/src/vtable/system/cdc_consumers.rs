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
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes CDC consumer checkpoint information
pub struct CdcConsumers {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl CdcConsumers {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_cdc_consumers_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for CdcConsumers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
