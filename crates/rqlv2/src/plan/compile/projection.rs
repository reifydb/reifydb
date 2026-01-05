// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Projection compilation helper.

use super::core::{Planner, Result};
use crate::{
	ast::{Expr, expr::BinaryOp},
	plan::{OutputSchema, node::query::Projection},
};

impl<'bump, 'cat> Planner<'bump, 'cat> {
	/// Compile a projection (expression with optional alias).
	pub(super) fn compile_projection(
		&self,
		expr: &Expr<'bump>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Projection<'bump>> {
		// Check if expression is aliased (Binary with KeyValue or As operator)
		match expr {
			Expr::Binary(bin) if bin.op == BinaryOp::KeyValue || bin.op == BinaryOp::As => {
				// Left is the alias name, right is the expression
				let alias = match bin.left {
					Expr::Identifier(ident) => Some(self.bump.alloc_str(ident.name) as &'bump str),
					_ => None,
				};
				let compiled = self.compile_expr(bin.right, schema)?;
				Ok(Projection {
					expr: compiled,
					alias,
					span: bin.span,
				})
			}
			_ => {
				let compiled = self.compile_expr(expr, schema)?;
				Ok(Projection {
					expr: compiled,
					alias: None,
					span: expr.span(),
				})
			}
		}
	}
}
