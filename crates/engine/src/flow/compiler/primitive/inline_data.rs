// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of inline data operations

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	expression::{AliasExpression, IdentExpression},
	flow::{conversion::to_owned_expression, node::FlowNodeType},
	plan::physical::InlineDataNode,
};
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct InlineDataCompiler {
	pub _inline_data: InlineDataNode,
}

impl From<InlineDataNode> for InlineDataCompiler {
	fn from(inline_data: InlineDataNode) -> Self {
		// Convert InlineDataNode to InlineDataNode
		let converted_rows = inline_data
			.rows
			.into_iter()
			.map(|row| {
				row.into_iter()
					.map(|alias_expr| AliasExpression {
						alias: IdentExpression(alias_expr.alias.0),
						expression: Box::new(to_owned_expression(*alias_expr.expression)),
						fragment: alias_expr.fragment,
					})
					.collect()
			})
			.collect();

		Self {
			_inline_data: InlineDataNode {
				rows: converted_rows,
			},
		}
	}
}

impl CompileOperator for InlineDataCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut CommandTransaction) -> Result<FlowNodeId> {
		compiler.add_node(txn, FlowNodeType::SourceInlineData {})
	}
}
