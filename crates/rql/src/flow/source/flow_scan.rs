// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of flow scan operations

use FlowNodeType::SourceFlow;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{CompileOperator, FlowCompiler, FlowNodeType};
use crate::{Result, plan::physical::FlowScanNode};

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

impl<T: CommandTransaction> CompileOperator<T> for FlowScanCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		compiler.add_node(SourceFlow {
			flow: self.flow_scan.source.def().id,
		})
		.await
	}
}
