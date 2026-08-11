// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::vtable::VTable, flow::OperatorCapability},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

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
		let mut operators = ColumnBuffer::utf8_with_capacity(capacity);
		let mut library_paths = ColumnBuffer::utf8_with_capacity(capacity);
		let mut abis = ColumnBuffer::uint4_with_capacity(capacity);
		let mut cap_inserts = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_updates = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_deletes = ColumnBuffer::bool_with_capacity(capacity);

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
			ColumnWithName::new(Fragment::internal("operator"), operators),
			ColumnWithName::new(Fragment::internal("library_path"), library_paths),
			ColumnWithName::new(Fragment::internal("abi"), abis),
			ColumnWithName::new(Fragment::internal("cap_insert"), cap_inserts),
			ColumnWithName::new(Fragment::internal("cap_update"), cap_updates),
			ColumnWithName::new(Fragment::internal("cap_delete"), cap_deletes),
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
