// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, value::value_type::ValueType};

use crate::{
	CatalogStore,
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
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<(), ValueError> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>, ValueError> {
		if self.exhausted {
			return Ok(None);
		}

		let operators = CatalogStore::list_operators_all(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, operators.len());
		let mut flow_ids = ColumnBuilder::with_capacity(ValueType::Uint8, operators.len());
		let mut kinds = ColumnBuilder::with_capacity(ValueType::Utf8, operators.len());

		for (flow, operator) in operators {
			ids.push(operator.id.0);
			flow_ids.push(flow.0);
			kinds.push(operator.ty.label());
		}

		let columns = vec![ids.finish("id"), flow_ids.finish("flow_id"), kinds.finish("kind")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
