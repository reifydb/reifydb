// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, flow::FlowLagsProvider},
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes per-source lag for each flow.
///
/// Each row shows how far behind a flow is for a specific source primitive.
pub struct SystemFlowLags {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	ioc: IocContainer,
}

impl SystemFlowLags {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_lags_table().clone(),
			exhausted: false,
			ioc,
		}
	}
}

impl BaseVTable for SystemFlowLags {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let rows = match self.ioc.resolve::<Arc<dyn FlowLagsProvider>>() {
			Ok(provider) => provider.all_lags(),
			Err(_) => vec![],
		};

		let mut flow_ids = ColumnData::uint8_with_capacity(rows.len());
		let mut primitive_ids = ColumnData::uint8_with_capacity(rows.len());
		let mut lags = ColumnData::uint8_with_capacity(rows.len());

		for row in rows {
			flow_ids.push(row.flow_id.0);
			primitive_ids.push(row.shape_id.as_u64());
			lags.push(row.lag);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("flow_id"),
				data: flow_ids,
			},
			Column {
				name: Fragment::internal("shape_id"),
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

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
