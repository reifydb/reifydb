// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnShape};
use reifydb_rql::{
	expression::{
		AddExpression, AliasExpression, AndExpression, BetweenExpression, CallExpression, CastExpression,
		ColumnExpression, ContainsExpression, DivExpression, ElseIfExpression, EqExpression, Expression,
		ExtendExpression, FieldAccessExpression, GreaterThanEqExpression, GreaterThanExpression, IfExpression,
		InExpression, LessThanEqExpression, LessThanExpression, ListExpression, MapExpression, MulExpression,
		NotEqExpression, OrExpression, PrefixExpression, RemExpression, SubExpression, TupleExpression,
		XorExpression,
	},
	instruction::CompiledFunction,
};
use reifydb_type::fragment::Fragment;

use crate::vm::stack::SymbolTable;

pub(crate) struct ExtractedUdf {
	pub func_def: CompiledFunction,
	pub arg_expressions: Vec<Expression>,
	pub result_column: Fragment,
}

pub(crate) fn extract_udf_calls(
	expr: &Expression,
	symbols: &SymbolTable,
	counter: &mut usize,
) -> (Expression, Vec<ExtractedUdf>) {
	let mut extracted = Vec::new();
	let rewritten = rewrite_expr(expr, symbols, counter, &mut extracted);
	(rewritten, extracted)
}

fn rw(e: &Expression, s: &SymbolTable, c: &mut usize, x: &mut Vec<ExtractedUdf>) -> Expression {
	rewrite_expr(e, s, c, x)
}

fn rw_vec(exprs: &[Expression], s: &SymbolTable, c: &mut usize, x: &mut Vec<ExtractedUdf>) -> Vec<Expression> {
	exprs.iter().map(|e| rewrite_expr(e, s, c, x)).collect()
}

fn rewrite_expr(
	expr: &Expression,
	symbols: &SymbolTable,
	counter: &mut usize,
	extracted: &mut Vec<ExtractedUdf>,
) -> Expression {
	match expr {
		Expression::Call(call) => {
			let rewritten_args = rw_vec(&call.args, symbols, counter, extracted);
			let function_name = call.func.0.text();

			if let Some(func_def) = symbols.get_function(function_name) {
				let col_name = Fragment::internal(format!("__udf_{}", counter));
				*counter += 1;

				extracted.push(ExtractedUdf {
					func_def: func_def.clone(),
					arg_expressions: rewritten_args,
					result_column: col_name.clone(),
				});

				Expression::Column(ColumnExpression(ColumnIdentifier {
					shape: ColumnShape::Alias(Fragment::internal("")),
					name: col_name,
				}))
			} else {
				Expression::Call(CallExpression {
					func: call.func.clone(),
					args: rewritten_args,
					fragment: call.fragment.clone(),
				})
			}
		}

		Expression::Add(e) => Expression::Add(AddExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Sub(e) => Expression::Sub(SubExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Mul(e) => Expression::Mul(MulExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Div(e) => Expression::Div(DivExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Rem(e) => Expression::Rem(RemExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::GreaterThan(e) => Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::GreaterThanEqual(e) => Expression::GreaterThanEqual(GreaterThanEqExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::LessThan(e) => Expression::LessThan(LessThanExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::LessThanEqual(e) => Expression::LessThanEqual(LessThanEqExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Equal(e) => Expression::Equal(EqExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::NotEqual(e) => Expression::NotEqual(NotEqExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::And(e) => Expression::And(AndExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Or(e) => Expression::Or(OrExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Xor(e) => Expression::Xor(XorExpression {
			left: Box::new(rw(&e.left, symbols, counter, extracted)),
			right: Box::new(rw(&e.right, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),

		Expression::Between(e) => Expression::Between(BetweenExpression {
			value: Box::new(rw(&e.value, symbols, counter, extracted)),
			lower: Box::new(rw(&e.lower, symbols, counter, extracted)),
			upper: Box::new(rw(&e.upper, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::In(e) => Expression::In(InExpression {
			value: Box::new(rw(&e.value, symbols, counter, extracted)),
			list: Box::new(rw(&e.list, symbols, counter, extracted)),
			negated: e.negated,
			fragment: e.fragment.clone(),
		}),
		Expression::Contains(e) => Expression::Contains(ContainsExpression {
			value: Box::new(rw(&e.value, symbols, counter, extracted)),
			list: Box::new(rw(&e.list, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Cast(e) => Expression::Cast(CastExpression {
			expression: Box::new(rw(&e.expression, symbols, counter, extracted)),
			to: e.to.clone(),
			fragment: e.fragment.clone(),
		}),
		Expression::Prefix(e) => Expression::Prefix(PrefixExpression {
			operator: e.operator.clone(),
			expression: Box::new(rw(&e.expression, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::Alias(e) => Expression::Alias(AliasExpression {
			alias: e.alias.clone(),
			expression: Box::new(rw(&e.expression, symbols, counter, extracted)),
			fragment: e.fragment.clone(),
		}),
		Expression::If(e) => Expression::If(IfExpression {
			condition: Box::new(rw(&e.condition, symbols, counter, extracted)),
			then_expr: Box::new(rw(&e.then_expr, symbols, counter, extracted)),
			else_ifs: e
				.else_ifs
				.iter()
				.map(|ei| ElseIfExpression {
					condition: Box::new(rw(&ei.condition, symbols, counter, extracted)),
					then_expr: Box::new(rw(&ei.then_expr, symbols, counter, extracted)),
					fragment: ei.fragment.clone(),
				})
				.collect(),
			else_expr: e.else_expr.as_ref().map(|b| Box::new(rw(b, symbols, counter, extracted))),
			fragment: e.fragment.clone(),
		}),
		Expression::FieldAccess(e) => Expression::FieldAccess(FieldAccessExpression {
			object: Box::new(rw(&e.object, symbols, counter, extracted)),
			field: e.field.clone(),
			fragment: e.fragment.clone(),
		}),

		Expression::Tuple(e) => Expression::Tuple(TupleExpression {
			expressions: rw_vec(&e.expressions, symbols, counter, extracted),
			fragment: e.fragment.clone(),
		}),
		Expression::List(e) => Expression::List(ListExpression {
			expressions: rw_vec(&e.expressions, symbols, counter, extracted),
			fragment: e.fragment.clone(),
		}),
		Expression::Map(e) => Expression::Map(MapExpression {
			expressions: rw_vec(&e.expressions, symbols, counter, extracted),
			fragment: e.fragment.clone(),
		}),
		Expression::Extend(e) => Expression::Extend(ExtendExpression {
			expressions: rw_vec(&e.expressions, symbols, counter, extracted),
			fragment: e.fragment.clone(),
		}),

		Expression::Constant(_)
		| Expression::Column(_)
		| Expression::AccessSource(_)
		| Expression::Parameter(_)
		| Expression::Variable(_)
		| Expression::Type(_)
		| Expression::SumTypeConstructor(_)
		| Expression::IsVariant(_) => expr.clone(),
	}
}
