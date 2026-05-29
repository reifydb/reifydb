// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType::SourceView, nodes::ViewScanNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

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

impl CompileOperator for ViewScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		compiler.add_node(
			txn,
			SourceView {
				view: self.view_scan.source.def().id(),
			},
		)
	}
}
