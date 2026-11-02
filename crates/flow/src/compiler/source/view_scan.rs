// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::flow::flow_queue_view_source_not_supported,
	flow::operator::OperatorDef::SourceView,
	interface::catalog::{flow::OperatorId, view::ViewStorageKind},
};
use reifydb_rql::nodes::ViewScanNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

use crate::compiler::{CompileOperator, FlowCompiler};

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		if self.view_scan.source.def().storage_kind() == ViewStorageKind::Queue {
			return Err(Error(Box::new(flow_queue_view_source_not_supported(
				self.view_scan.source.def().name(),
			))));
		}
		compiler.add_node(
			txn,
			SourceView {
				view: self.view_scan.source.def().id(),
			},
		)
	}
}
