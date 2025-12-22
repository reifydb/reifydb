// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of view scan operations

use FlowNodeType::SourceView;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{CompileOperator, FlowCompiler, FlowNodeType};
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

impl<T: CommandTransaction> CompileOperator<T> for ViewScanCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		compiler.build_node(SourceView {
			view: self.view_scan.source.def().id,
		})
		.build()
		.await
	}
}
