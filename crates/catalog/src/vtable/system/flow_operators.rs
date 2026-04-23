// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_abi::operator::capabilities::{
	CAPABILITY_DELETE, CAPABILITY_DROP, CAPABILITY_INSERT, CAPABILITY_PULL, CAPABILITY_TICK, CAPABILITY_UPDATE,
	has_capability,
};
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

/// Virtual table that exposes loaded FFI operators from shared libraries
pub struct SystemFlowOperators {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	flow_operator_store: SystemFlowOperatorStore,
}

impl SystemFlowOperators {
	pub fn new(flow_operator_store: SystemFlowOperatorStore) -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_operators_table().clone(),
			exhausted: false,
			flow_operator_store,
		}
	}
}

impl BaseVTable for SystemFlowOperators {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let infos = self.flow_operator_store.list();

		let capacity = infos.len();
		let mut operators = ColumnBuffer::utf8_with_capacity(capacity);
		let mut library_paths = ColumnBuffer::utf8_with_capacity(capacity);
		let mut apis = ColumnBuffer::uint4_with_capacity(capacity);
		let mut cap_inserts = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_updates = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_deletes = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_pull_list = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_drops = ColumnBuffer::bool_with_capacity(capacity);
		let mut cap_ticks = ColumnBuffer::bool_with_capacity(capacity);

		for info in infos {
			operators.push(info.operator.as_str());
			library_paths.push(info.library_path.to_str().unwrap_or("<invalid path>"));
			apis.push(info.api);

			// Decode capabilities bitfield into separate boolean columns
			cap_inserts.push(has_capability(info.capabilities, CAPABILITY_INSERT));
			cap_updates.push(has_capability(info.capabilities, CAPABILITY_UPDATE));
			cap_deletes.push(has_capability(info.capabilities, CAPABILITY_DELETE));
			cap_pull_list.push(has_capability(info.capabilities, CAPABILITY_PULL));
			cap_drops.push(has_capability(info.capabilities, CAPABILITY_DROP));
			cap_ticks.push(has_capability(info.capabilities, CAPABILITY_TICK));
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("operator"), operators),
			ColumnWithName::new(Fragment::internal("library_path"), library_paths),
			ColumnWithName::new(Fragment::internal("api"), apis),
			ColumnWithName::new(Fragment::internal("cap_insert"), cap_inserts),
			ColumnWithName::new(Fragment::internal("cap_update"), cap_updates),
			ColumnWithName::new(Fragment::internal("cap_delete"), cap_deletes),
			ColumnWithName::new(Fragment::internal("cap_pull"), cap_pull_list),
			ColumnWithName::new(Fragment::internal("cap_drop"), cap_drops),
			ColumnWithName::new(Fragment::internal("cap_tick"), cap_ticks),
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
