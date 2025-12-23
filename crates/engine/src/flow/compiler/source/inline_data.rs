// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of inline data operations

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	expression::{AliasExpression, IdentExpression},
	flow::{FlowNodeType, conversion::to_owned_expression},
	plan::physical::InlineDataNode,
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		compiler.add_node(txn, FlowNodeType::SourceInlineData {}).await
	}
}
