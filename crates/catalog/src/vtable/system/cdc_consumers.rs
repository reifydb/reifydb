// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::VTableDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

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

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for CdcConsumers {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
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
