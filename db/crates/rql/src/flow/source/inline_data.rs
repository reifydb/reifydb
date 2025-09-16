// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of inline data operations

use reifydb_core::{
	flow::FlowNodeType,
	interface::{
		CommandTransaction, FlowNodeId,
		evaluate::expression::{AliasExpression, IdentExpression},
	},
};
use reifydb_type::Fragment;

use super::super::{CompileOperator, FlowCompiler, conversion::to_owned_expression};
use crate::{Result, plan::physical::InlineDataNode};

pub(crate) struct InlineDataCompiler {
	pub inline_data: InlineDataNode<'static>,
}

impl<'a> From<InlineDataNode<'a>> for InlineDataCompiler {
	fn from(inline_data: InlineDataNode<'a>) -> Self {
		// Convert InlineDataNode<'a> to InlineDataNode<'static>
		let converted_rows = inline_data
			.rows
			.into_iter()
			.map(|row| {
				row.into_iter()
					.map(|alias_expr| AliasExpression {
						alias: IdentExpression(Fragment::Owned(
							alias_expr.alias.0.into_owned(),
						)),
						expression: Box::new(to_owned_expression(*alias_expr.expression)),
						fragment: Fragment::Owned(alias_expr.fragment.into_owned()),
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
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		compiler.build_node(FlowNodeType::SourceInlineData {}).build()
	}
}
