// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of view scan operations

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{FlowNodeSchema, FlowNodeType},
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{Result, plan::physical::ViewScanNode};

pub(crate) struct ViewScanCompiler {
	pub view_scan: ViewScanNode,
}

impl From<ViewScanNode> for ViewScanCompiler {
	fn from(view_scan: ViewScanNode) -> Self {
		Self {
			view_scan,
		}
	}
}

impl<'a, T: CommandTransaction> CompileOperator<T> for ViewScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let view = self.view_scan.view;
		let view_name = view.name.clone();

		// Get schema information
		let schema_def = CatalogStore::get_schema(
			unsafe { &mut *compiler.txn },
			view.schema,
		)?;

		let schema = FlowNodeSchema::new(
			view.columns.clone(),
			Some(schema_def.name.clone()),
			Some(view.name.clone()),
		);

		compiler.build_node(FlowNodeType::SourceView {
			name: view_name,
			view: view.id,
			schema,
		})
		.build()
	}
}
