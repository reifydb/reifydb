// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of view scan operations

use reifydb_core::{
	flow::{FlowNodeDef, FlowNodeType},
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
		let view = self.view_scan.source.def().clone();
		let view_name = view.name.clone();

		// Get namespace information
		let namespace_def = self.view_scan.source.namespace().def().clone();

		let namespace = FlowNodeDef::new(
			view.columns.clone(),
			Some(namespace_def.name.clone()),
			Some(view.name.clone()),
		);

		compiler.build_node(FlowNodeType::SourceView {
			name: view_name,
			view: view.id,
			namespace,
		})
		.build()
	}
}
