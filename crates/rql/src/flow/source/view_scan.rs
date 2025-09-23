// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of view scan operations

use FlowNodeType::SourceView;
use reifydb_core::{
	flow::FlowNodeType,
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{Result, plan::physical::ViewScanNode};

pub(crate) struct ViewScanCompiler<'a> {
	pub view_scan: ViewScanNode<'a>,
}

impl<'a> From<ViewScanNode<'a>> for ViewScanCompiler<'a> {
	fn from(view_scan: ViewScanNode<'a>) -> Self {
		Self {
			view_scan,
		}
	}
}

impl<'a, T: CommandTransaction> CompileOperator<T> for ViewScanCompiler<'a> {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		compiler.build_node(SourceView {
			view: self.view_scan.source.def().id,
		})
		.build()
	}
}
