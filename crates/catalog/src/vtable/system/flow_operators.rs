// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_abi::operator::capabilities::{
	CAPABILITY_DELETE, CAPABILITY_DROP, CAPABILITY_INSERT, CAPABILITY_PULL, CAPABILITY_TICK, CAPABILITY_UPDATE,
	has_capability,
};
use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::flow_operator_store::FlowOperatorStore;
use crate::{
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes loaded FFI operators from shared libraries
pub struct FlowOperators {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
	flow_operator_store: FlowOperatorStore,
}

impl FlowOperators {
	pub fn new(flow_operator_store: FlowOperatorStore) -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_operators_table_def().clone(),
			exhausted: false,
			flow_operator_store,
		}
	}
}

impl VTable for FlowOperators {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let infos = self.flow_operator_store.list();

		let capacity = infos.len();
		let mut operators = ColumnData::utf8_with_capacity(capacity);
		let mut library_paths = ColumnData::utf8_with_capacity(capacity);
		let mut apis = ColumnData::uint4_with_capacity(capacity);
		let mut cap_inserts = ColumnData::bool_with_capacity(capacity);
		let mut cap_updates = ColumnData::bool_with_capacity(capacity);
		let mut cap_deletes = ColumnData::bool_with_capacity(capacity);
		let mut cap_pull_list = ColumnData::bool_with_capacity(capacity);
		let mut cap_drops = ColumnData::bool_with_capacity(capacity);
		let mut cap_ticks = ColumnData::bool_with_capacity(capacity);

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
			Column {
				name: Fragment::internal("operator"),
				data: operators,
			},
			Column {
				name: Fragment::internal("library_path"),
				data: library_paths,
			},
			Column {
				name: Fragment::internal("api"),
				data: apis,
			},
			Column {
				name: Fragment::internal("cap_insert"),
				data: cap_inserts,
			},
			Column {
				name: Fragment::internal("cap_update"),
				data: cap_updates,
			},
			Column {
				name: Fragment::internal("cap_delete"),
				data: cap_deletes,
			},
			Column {
				name: Fragment::internal("cap_pull"),
				data: cap_pull_list,
			},
			Column {
				name: Fragment::internal("cap_drop"),
				data: cap_drops,
			},
			Column {
				name: Fragment::internal("cap_tick"),
				data: cap_ticks,
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
