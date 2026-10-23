// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, flow::FlowUnitSampler},
	util::ioc::IocContainer,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemFlowUnits {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	ioc: IocContainer,
}

impl SystemFlowUnits {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_units_table().clone(),
			exhausted: false,
			ioc,
		}
	}
}

impl BaseVTable for SystemFlowUnits {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let rows = match self.ioc.try_resolve::<FlowUnitSampler>() {
			Some(source) => source.all(),
			None => vec![],
		};

		let mut flow_ids = ColumnBuilder::with_capacity(ValueType::Uint8, rows.len());
		let mut units = ColumnBuilder::with_capacity(ValueType::Uint8, rows.len());

		for row in rows {
			flow_ids.push(row.flow_id.0);
			units.push(row.unit.0);
		}

		let columns = vec![flow_ids.finish("flow_id"), units.finish("unit")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
