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

		// Access the flow operator store
		let operators = self.flow_operator_store.list();

		// Pre-allocate vectors for column data
		let capacity = operators.len();
		let mut operator_names = ColumnData::utf8_with_capacity(capacity);
		let mut library_paths = ColumnData::utf8_with_capacity(capacity);
		let mut api_versions = ColumnData::uint4_with_capacity(capacity);

		// Populate column data from loaded operators
		for operator_info in operators {
			operator_names.push(operator_info.operator_name.as_str());
			library_paths.push(operator_info.library_path.to_str().unwrap_or("<invalid path>"));
			api_versions.push(operator_info.api_version);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("operator_name"),
				data: operator_names,
			},
			Column {
				name: Fragment::owned_internal("library_path"),
				data: library_paths,
			},
			Column {
				name: Fragment::owned_internal("api_version"),
				data: api_versions,
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
