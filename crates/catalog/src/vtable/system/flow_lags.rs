// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{FlowLagsProvider, VTableDef},
	ioc::IocContainer,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

use crate::{
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes per-source lag for each flow.
///
/// Each row shows how far behind a flow is for a specific source primitive.
pub struct FlowLags {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
	ioc: IocContainer,
}

impl FlowLags {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_lags_table_def().clone(),
			exhausted: false,
			ioc,
		}
	}
}

impl<T: IntoStandardTransaction> VTable<T> for FlowLags {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Lazily resolve the provider from IoC - if not registered yet, return empty
		let rows = match self.ioc.resolve::<Arc<dyn FlowLagsProvider>>() {
			Ok(provider) => provider.all_lags(),
			Err(_) => vec![],
		};

		let mut flow_ids = ColumnData::uint8_with_capacity(rows.len());
		let mut primitive_ids = ColumnData::uint8_with_capacity(rows.len());
		let mut lags = ColumnData::uint8_with_capacity(rows.len());

		for row in rows {
			flow_ids.push(row.flow_id.0);
			primitive_ids.push(row.primitive_id.as_u64());
			lags.push(row.lag);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("flow_id"),
				data: flow_ids,
			},
			Column {
				name: Fragment::internal("primitive_id"),
				data: primitive_ids,
			},
			Column {
				name: Fragment::internal("lag"),
				data: lags,
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
