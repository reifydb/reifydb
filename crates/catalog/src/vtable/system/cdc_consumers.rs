// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{Batch, QueryTransaction, VTableDef, get_all_consumer_states},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	system::SystemCatalog,
	vtable::{VTable, VTableContext},
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
impl<T: QueryTransaction> VTable<T> for CdcConsumers {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let consumer_states = get_all_consumer_states(txn).await?;

		let mut consumer_ids = ColumnData::utf8_with_capacity(consumer_states.len());
		let mut checkpoints = ColumnData::uint8_with_capacity(consumer_states.len());

		for state in consumer_states {
			consumer_ids.push(state.consumer_id.as_ref());
			checkpoints.push(state.checkpoint.0);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("consumer_id"),
				data: consumer_ids,
			},
			Column {
				name: Fragment::internal("checkpoint"),
				data: checkpoints,
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
