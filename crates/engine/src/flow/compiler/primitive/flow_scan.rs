// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of flow scan operations

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType::SourceFlow, nodes::FlowScanNode};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct FlowScanCompiler {
	pub flow_scan: FlowScanNode,
}

impl From<FlowScanNode> for FlowScanCompiler {
	fn from(flow_scan: FlowScanNode) -> Self {
		Self {
			flow_scan,
		}
	}
}

impl CompileOperator for FlowScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		compiler.add_node(
			txn,
			SourceFlow {
				flow: self.flow_scan.source.def().id,
			},
		)
	}
}
