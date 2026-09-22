// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, flow::OperatorCapability},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use super::operator_libary::OperatorLibrary;
use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemOperatorLibraries {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	operator_store: OperatorLibrary,
}

impl SystemOperatorLibraries {
	pub fn new(operator_store: OperatorLibrary) -> Self {
		Self {
			vtable: SystemCatalog::get_system_operator_libraries_table().clone(),
			exhausted: false,
			operator_store,
		}
	}
}

impl BaseVTable for SystemOperatorLibraries {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let infos = self.operator_store.list();

		let capacity = infos.len();
		let mut operators = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
		let mut library_paths = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
		let mut abis = ColumnBuilder::with_capacity(ValueType::Uint4, capacity);
		let mut cap_inserts = ColumnBuilder::with_capacity(ValueType::Boolean, capacity);
		let mut cap_updates = ColumnBuilder::with_capacity(ValueType::Boolean, capacity);
		let mut cap_deletes = ColumnBuilder::with_capacity(ValueType::Boolean, capacity);

		for info in infos {
			operators.push(info.operator.as_str());
			library_paths.push(info.library_path.to_str().unwrap_or("<invalid path>"));
			match info.abi {
				Some(abi) => abis.push(abi),
				None => abis.push_none(),
			}

			cap_inserts.push(info.capabilities & OperatorCapability::Insert.bit() != 0);
			cap_updates.push(info.capabilities & OperatorCapability::Update.bit() != 0);
			cap_deletes.push(info.capabilities & OperatorCapability::Delete.bit() != 0);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("operator"), operators.finish()),
			ColumnWithName::new(Fragment::internal("library_path"), library_paths.finish()),
			ColumnWithName::new(Fragment::internal("abi"), abis.finish()),
			ColumnWithName::new(Fragment::internal("cap_insert"), cap_inserts.finish()),
			ColumnWithName::new(Fragment::internal("cap_update"), cap_updates.finish()),
			ColumnWithName::new(Fragment::internal("cap_delete"), cap_deletes.finish()),
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
