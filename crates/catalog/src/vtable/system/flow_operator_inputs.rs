// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::flow_operator_store::FlowOperatorStore;
use crate::{
	Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes input column definitions for FFI operators
pub struct FlowOperatorInputs {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
	flow_operator_store: FlowOperatorStore,
}

impl FlowOperatorInputs {
	pub fn new(flow_operator_store: FlowOperatorStore) -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_operator_inputs_table_def().clone(),
			exhausted: false,
			flow_operator_store,
		}
	}
}

impl VTable for FlowOperatorInputs {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Access the flow operator store
		let infos = self.flow_operator_store.list();

		// Count total input columns across all operators for capacity
		let capacity: usize = infos.iter().map(|op| op.input_columns.len()).sum();

		// Pre-allocate vectors for column data
		let mut operators = ColumnData::utf8_with_capacity(capacity);
		let mut positions = ColumnData::uint1_with_capacity(capacity);
		let mut names = ColumnData::utf8_with_capacity(capacity);
		let mut column_types = ColumnData::uint1_with_capacity(capacity);
		let mut descriptions = ColumnData::utf8_with_capacity(capacity);

		// Populate column data from loaded operators
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
			Column {
				name: Fragment::internal("operator"),
				data: operators,
			},
			Column {
				name: Fragment::internal("position"),
				data: positions,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("type"),
				data: column_types,
			},
			Column {
				name: Fragment::internal("description"),
				data: descriptions,
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
