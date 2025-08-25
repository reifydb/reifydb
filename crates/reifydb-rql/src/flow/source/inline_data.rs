// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of inline data operations

use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{CompileOperator, FlowCompiler};
use crate::{Result, plan::physical::InlineDataNode};

pub(crate) struct InlineDataCompiler {
	pub inline_data: InlineDataNode,
}

impl From<InlineDataNode> for InlineDataCompiler {
	fn from(inline_data: InlineDataNode) -> Self {
		Self {
			inline_data,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for InlineDataCompiler {
	fn compile(
		self,
		_compiler: &mut FlowCompiler<T>,
	) -> Result<FlowNodeId> {
		// TODO: Implement inline data compilation
		unimplemented!("Inline data compilation not yet implemented")
	}
}
