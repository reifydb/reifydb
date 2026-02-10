// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod context;
mod execute;
pub mod expr;

pub use context::{CompileContext, ExecContext};
pub use expr::CompiledExpr;
use reifydb_rql::expression::Expression;

/// Compile an `Expression` into a `CompiledExpr`.
///
/// Recursively compiles sub-expressions into native `CompiledExpr` variants.
/// All expression types have native variants.
pub fn compile_expression(_ctx: &CompileContext, expr: &Expression) -> crate::Result<CompiledExpr> {
	Ok(match expr {
		Expression::Constant(e) => CompiledExpr::Constant(e.clone()),
		Expression::Column(e) => CompiledExpr::Column(e.clone()),
		Expression::Variable(e) => CompiledExpr::Variable(e.clone()),
		Expression::Parameter(e) => CompiledExpr::Parameter(e.clone()),
		Expression::Alias(e) => CompiledExpr::Alias {
			inner: Box::new(compile_expression(_ctx, &e.expression)?),
			alias: e.alias.0.clone(),
		},
		Expression::Add(e) => CompiledExpr::Add {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Sub(e) => CompiledExpr::Sub {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Mul(e) => CompiledExpr::Mul {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Div(e) => CompiledExpr::Div {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Rem(e) => CompiledExpr::Rem {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Equal(e) => CompiledExpr::Equal {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::NotEqual(e) => CompiledExpr::NotEqual {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::GreaterThan(e) => CompiledExpr::GreaterThan {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::GreaterThanEqual(e) => CompiledExpr::GreaterThanEqual {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::LessThan(e) => CompiledExpr::LessThan {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::LessThanEqual(e) => CompiledExpr::LessThanEqual {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::And(e) => CompiledExpr::And {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Or(e) => CompiledExpr::Or {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Xor(e) => CompiledExpr::Xor {
			left: Box::new(compile_expression(_ctx, &e.left)?),
			right: Box::new(compile_expression(_ctx, &e.right)?),
			fragment: e.full_fragment_owned(),
		},
		Expression::Prefix(e) => CompiledExpr::Prefix(e.clone()),
		Expression::Type(e) => CompiledExpr::Type {
			ty: e.ty,
			fragment: e.fragment.clone(),
		},
		Expression::AccessSource(e) => CompiledExpr::AccessSource(e.clone()),
		Expression::Tuple(e) => {
			if e.expressions.len() == 1 {
				CompiledExpr::Tuple {
					inner: Box::new(compile_expression(_ctx, &e.expressions[0])?),
				}
			} else {
				unimplemented!("Multi-element tuple evaluation not yet supported: {:?}", e)
			}
		}
		Expression::Between(e) => CompiledExpr::Between {
			value: Box::new(compile_expression(_ctx, &e.value)?),
			lower: Box::new(compile_expression(_ctx, &e.lower)?),
			upper: Box::new(compile_expression(_ctx, &e.upper)?),
			fragment: e.fragment.clone(),
		},
		Expression::In(e) => {
			let list_expressions = match e.list.as_ref() {
				Expression::Tuple(tuple) => &tuple.expressions,
				_ => std::slice::from_ref(e.list.as_ref()),
			};
			CompiledExpr::In {
				value: Box::new(compile_expression(_ctx, &e.value)?),
				list: list_expressions
					.iter()
					.map(|expr| compile_expression(_ctx, expr))
					.collect::<crate::Result<Vec<_>>>()?,
				negated: e.negated,
				fragment: e.fragment.clone(),
			}
		}
		Expression::Cast(e) => CompiledExpr::Cast {
			inner: Box::new(compile_expression(_ctx, &e.expression)?),
			target_type: e.to.ty,
			inner_fragment: e.expression.full_fragment_owned(),
		},
		Expression::If(e) => CompiledExpr::If {
			condition: Box::new(compile_expression(_ctx, &e.condition)?),
			then_expr: compile_expressions(_ctx, std::slice::from_ref(e.then_expr.as_ref()))?,
			else_ifs: e
				.else_ifs
				.iter()
				.map(|ei| {
					Ok((
						Box::new(compile_expression(_ctx, &ei.condition)?),
						compile_expressions(_ctx, std::slice::from_ref(ei.then_expr.as_ref()))?,
					))
				})
				.collect::<crate::Result<Vec<_>>>()?,
			else_branch: match &e.else_expr {
				Some(expr) => Some(compile_expressions(_ctx, std::slice::from_ref(expr.as_ref()))?),
				None => None,
			},
			fragment: e.fragment.clone(),
		},
		Expression::Map(e) => CompiledExpr::Map {
			expressions: compile_expressions(_ctx, &e.expressions)?,
		},
		Expression::Extend(e) => CompiledExpr::Extend {
			expressions: compile_expressions(_ctx, &e.expressions)?,
		},
		Expression::Call(e) => CompiledExpr::Call(e.clone()),
	})
}

fn compile_expressions(ctx: &CompileContext, exprs: &[Expression]) -> crate::Result<Vec<CompiledExpr>> {
	exprs.iter().map(|e| compile_expression(ctx, e)).collect()
}
