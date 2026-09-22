// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemOperators {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl SystemOperators {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_operators_table().clone(),
			exhausted: false,
		}
	}
}

impl Default for SystemOperators {
	fn default() -> Self {
		Self::new()
	}
}

impl BaseVTable for SystemOperators {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let operators = CatalogStore::list_operators_all(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, operators.len());
		let mut flow_ids = ColumnBuilder::with_capacity(ValueType::Uint8, operators.len());
		let mut node_types = ColumnBuilder::with_capacity(ValueType::Uint1, operators.len());
		let mut data_column = ColumnBuilder::with_capacity(ValueType::Blob, operators.len());

		for operator in operators {
			ids.push(operator.id.0);
			flow_ids.push(operator.flow.0);
			node_types.push(operator.node_type);
			data_column.push(operator.data);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("flow_id"), flow_ids.finish()),
			ColumnWithName::new(Fragment::internal("node_type"), node_types.finish()),
			ColumnWithName::new(Fragment::internal("data"), data_column.finish()),
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
