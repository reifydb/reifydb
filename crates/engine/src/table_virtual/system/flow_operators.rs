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

		for info in infos {
			operators.push(info.operator.as_str());
			library_paths.push(info.library_path.to_str().unwrap_or("<invalid path>"));
			apis.push(info.api);
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
