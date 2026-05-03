// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::flow_operator_store::SystemFlowOperatorStore;
use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemFlowOperatorInputs {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	flow_operator_store: SystemFlowOperatorStore,
}

impl SystemFlowOperatorInputs {
	pub fn new(flow_operator_store: SystemFlowOperatorStore) -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_operator_inputs_table().clone(),
			exhausted: false,
			flow_operator_store,
		}
	}
}

impl BaseVTable for SystemFlowOperatorInputs {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let infos = self.flow_operator_store.list();

		let capacity: usize = infos.iter().map(|op| op.input_columns.len()).sum();

		let mut operators = ColumnBuffer::utf8_with_capacity(capacity);
		let mut positions = ColumnBuffer::uint1_with_capacity(capacity);
		let mut names = ColumnBuffer::utf8_with_capacity(capacity);
		let mut column_types = ColumnBuffer::uint1_with_capacity(capacity);
		let mut descriptions = ColumnBuffer::utf8_with_capacity(capacity);

		for info in infos {
			for (position, col) in info.input_columns.iter().enumerate() {
				operators.push(info.operator.as_str());
				positions.push(position as u8);
				names.push(col.name.as_str());
				column_types.push(col.field_type.get_type().to_u8());
				descriptions.push(col.description.as_str());
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("operator"), operators),
			ColumnWithName::new(Fragment::internal("position"), positions),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("type"), column_types),
			ColumnWithName::new(Fragment::internal("description"), descriptions),
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
