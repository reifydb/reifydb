// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::system::SystemCatalog;
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, get_all_consumer_states},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes CDC consumer checkpoint information
pub struct CdcConsumers {
	definition: Arc<TableVirtualDef>,
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

impl<'a> TableVirtual<'a> for CdcConsumers {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let consumer_states = get_all_consumer_states(txn)?;

		let mut consumer_ids = ColumnData::utf8_with_capacity(consumer_states.len());
		let mut checkpoints = ColumnData::uint8_with_capacity(consumer_states.len());

		for state in consumer_states {
			consumer_ids.push(state.consumer_id.as_ref());
			checkpoints.push(state.checkpoint.0);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("consumer_id"),
				data: consumer_ids,
			},
			Column {
				name: Fragment::owned_internal("checkpoint"),
				data: checkpoints,
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
