// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_rql::{flow::operator::OperatorDef, nodes::InlineDataNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct InlineDataCompiler {
	pub _inline_data: InlineDataNode,
}

impl From<InlineDataNode> for InlineDataCompiler {
	fn from(inline_data: InlineDataNode) -> Self {
		Self {
			_inline_data: inline_data,
		}
	}
}

impl CompileOperator for InlineDataCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		compiler.add_node(txn, OperatorDef::SourceInlineData {})
	}
}
