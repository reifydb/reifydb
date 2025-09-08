// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of view scan operations

use reifydb_core::{
	flow::FlowNodeType,
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

		compiler.build_node(FlowNodeType::SourceView {
			name: view_name,
			view: view.id,
		})
		.build()
	}
}
