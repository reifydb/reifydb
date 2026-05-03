// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, flow::FlowWatermarkSampler},
	util::ioc::IocContainer,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemFlowWatermarks {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	ioc: IocContainer,
}

impl SystemFlowWatermarks {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_watermarks_table().clone(),
			exhausted: false,
			ioc,
		}
	}
}

impl BaseVTable for SystemFlowWatermarks {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let rows = match self.ioc.resolve::<FlowWatermarkSampler>() {
			Ok(source) => source.all(),
			Err(_) => vec![],
		};

		let mut flow_ids = ColumnBuffer::uint8_with_capacity(rows.len());
		let mut primitive_ids = ColumnBuffer::uint8_with_capacity(rows.len());
		let mut lags = ColumnBuffer::uint8_with_capacity(rows.len());

		for row in rows {
			flow_ids.push(row.flow_id.0);
			primitive_ids.push(row.shape_id.as_u64());
			lags.push(row.lag);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("flow_id"), flow_ids),
			ColumnWithName::new(Fragment::internal("shape_id"), primitive_ids),
			ColumnWithName::new(Fragment::internal("lag"), lags),
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
