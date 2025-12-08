// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::system::SystemCatalog;
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_flow_operator_abi::{
	CAPABILITY_DELETE, CAPABILITY_DROP, CAPABILITY_GET_ROWS, CAPABILITY_INSERT, CAPABILITY_TICK, CAPABILITY_UPDATE,
	has_capability,
};
use reifydb_type::Fragment;

use super::FlowOperatorStore;
use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes loaded FFI operators from shared libraries
pub struct FlowOperators {
	definition: Arc<TableVirtualDef>,
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

impl<'a> TableVirtual<'a> for FlowOperators {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
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
		let mut cap_get_rows_list = ColumnData::bool_with_capacity(capacity);
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
			cap_get_rows_list.push(has_capability(info.capabilities, CAPABILITY_GET_ROWS));
			cap_drops.push(has_capability(info.capabilities, CAPABILITY_DROP));
			cap_ticks.push(has_capability(info.capabilities, CAPABILITY_TICK));
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("operator"),
				data: operators,
			},
			Column {
				name: Fragment::owned_internal("library_path"),
				data: library_paths,
			},
			Column {
				name: Fragment::owned_internal("api"),
				data: apis,
			},
			Column {
				name: Fragment::owned_internal("cap_insert"),
				data: cap_inserts,
			},
			Column {
				name: Fragment::owned_internal("cap_update"),
				data: cap_updates,
			},
			Column {
				name: Fragment::owned_internal("cap_delete"),
				data: cap_deletes,
			},
			Column {
				name: Fragment::owned_internal("cap_get_rows"),
				data: cap_get_rows_list,
			},
			Column {
				name: Fragment::owned_internal("cap_drop"),
				data: cap_drops,
			},
			Column {
				name: Fragment::owned_internal("cap_tick"),
				data: cap_ticks,
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
