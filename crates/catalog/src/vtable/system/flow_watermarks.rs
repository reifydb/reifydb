// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, flow::FlowWatermarkSampler},
	util::ioc::IocContainer,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

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

		let rows = match self.ioc.try_resolve::<FlowWatermarkSampler>() {
			Some(source) => source.all(),
			None => vec![],
		};

		let mut flow_ids = ColumnBuilder::with_capacity(ValueType::Uint8, rows.len());
		let mut object_ids = ColumnBuilder::with_capacity(ValueType::Uint8, rows.len());
		let mut lags = ColumnBuilder::with_capacity(ValueType::Uint8, rows.len());
		let mut outstanding = ColumnBuilder::with_capacity(ValueType::Uint8, rows.len());

		for row in rows {
			flow_ids.push(row.flow_id.0);
			object_ids.push(row.object_id.as_u64());
			lags.push(row.lag);
			outstanding.push(row.outstanding);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("flow_id"), flow_ids.finish()),
			ColumnWithName::new(Fragment::internal("object_id"), object_ids.finish()),
			ColumnWithName::new(Fragment::internal("lag"), lags.finish()),
			ColumnWithName::new(Fragment::internal("outstanding"), outstanding.finish()),
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
