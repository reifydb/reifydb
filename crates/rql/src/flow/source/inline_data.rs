// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of inline data operations

use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{CompileOperator, FlowCompiler, FlowNodeType, conversion::to_owned_expression};
use crate::{
	Result,
	expression::{AliasExpression, IdentExpression},
	plan::physical::InlineDataNode,
};

pub(crate) struct InlineDataCompiler {
	pub inline_data: InlineDataNode,
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
						alias: IdentExpression(alias_expr.alias.0.into_owned()),
						expression: Box::new(to_owned_expression(*alias_expr.expression)),
						fragment: alias_expr.fragment,
					})
					.collect()
			})
			.collect();

		Self {
			inline_data: InlineDataNode {
				rows: converted_rows,
			},
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for InlineDataCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		compiler.build_node(FlowNodeType::SourceInlineData {}).build().await
	}
}
