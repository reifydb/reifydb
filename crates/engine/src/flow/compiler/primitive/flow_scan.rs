// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of flow scan operations

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{flow::FlowNodeType::SourceFlow, plan::physical::FlowScanNode};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		compiler.add_node(
			txn,
			SourceFlow {
				flow: self.flow_scan.source.def().id,
			},
		)
	}
}
