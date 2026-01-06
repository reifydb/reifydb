// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of view scan operations

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{flow::FlowNodeType::SourceView, plan::physical::ViewScanNode};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		compiler.add_node(
			txn,
			SourceView {
				view: self.view_scan.source.def().id,
			},
		)
	}
}
