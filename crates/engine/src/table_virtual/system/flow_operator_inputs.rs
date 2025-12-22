// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
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

/// Virtual table that exposes input column definitions for FFI operators
pub struct FlowOperatorInputs {
	definition: Arc<TableVirtualDef>,
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

#[async_trait]
impl TableVirtual for FlowOperatorInputs {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		_ctx: TableVirtualContext,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next<'a>(&mut self, _txn: &mut StandardTransaction<'a>) -> Result<Option<Batch>> {
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

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
