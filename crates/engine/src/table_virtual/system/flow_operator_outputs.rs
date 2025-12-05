// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::system::SystemCatalog;
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use super::FlowOperatorStore;
use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes output column definitions for FFI operators
pub struct FlowOperatorOutputs {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	flow_operator_store: FlowOperatorStore,
}

impl FlowOperatorOutputs {
	pub fn new(flow_operator_store: FlowOperatorStore) -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_operator_outputs_table_def().clone(),
			exhausted: false,
			flow_operator_store,
		}
	}
}

impl<'a> TableVirtual<'a> for FlowOperatorOutputs {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		// Access the flow operator store
		let operators = self.flow_operator_store.list();

		// Count total output columns across all operators for capacity
		let capacity: usize = operators.iter().map(|op| op.output_columns.len()).sum();

		// Pre-allocate vectors for column data
		let mut operator_names = ColumnData::utf8_with_capacity(capacity);
		let mut positions = ColumnData::uint1_with_capacity(capacity);
		let mut names = ColumnData::utf8_with_capacity(capacity);
		let mut column_types = ColumnData::uint1_with_capacity(capacity);
		let mut descriptions = ColumnData::utf8_with_capacity(capacity);

		// Populate column data from loaded operators
		for operator_info in operators {
			for (position, col) in operator_info.output_columns.iter().enumerate() {
				operator_names.push(operator_info.operator_name.as_str());
				positions.push(position as u8);
				names.push(col.name.as_str());
				column_types.push(col.field_type.get_type().to_u8());
				descriptions.push(col.description.as_str());
			}
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("operator"),
				data: operator_names,
			},
			Column {
				name: Fragment::owned_internal("position"),
				data: positions,
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: names,
			},
			Column {
				name: Fragment::owned_internal("type"),
				data: column_types,
			},
			Column {
				name: Fragment::owned_internal("description"),
				data: descriptions,
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
