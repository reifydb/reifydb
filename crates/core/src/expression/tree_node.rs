// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use datafusion_common::{
	Result,
	tree_node::{Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion},
};

use crate::expression::{
	AddExpression, AliasExpression, AndExpression, BetweenExpression, CallExpression, CastExpression,
	ContainsExpression, DivExpression, ElseIfExpression, EqExpression, Expression, ExtendExpression,
	FieldAccessExpression, GreaterThanEqExpression, GreaterThanExpression, IfExpression, InExpression,
	IsVariantExpression, LessThanEqExpression, LessThanExpression, ListExpression, MapExpression, MulExpression,
	NotEqExpression, OrExpression, PrefixExpression, RemExpression, SubExpression, SumTypeConstructorExpression,
	TupleExpression, XorExpression,
};

impl TreeNode for Expression {
	fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
		&'n self,
		f: F,
	) -> Result<TreeNodeRecursion> {
		children(self).into_iter().apply_until_stop(f)
	}

	fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(self, f: F) -> Result<Transformed<Self>> {
		let mut m = Children {
			f,
			transformed: false,
			tnr: TreeNodeRecursion::Continue,
		};
		let expr = match self {
			Expression::AccessSource(_)
			| Expression::Constant(_)
			| Expression::Column(_)
			| Expression::Type(_)
			| Expression::Parameter(_)
			| Expression::Variable(_) => return Ok(Transformed::no(self)),
			Expression::Alias(AliasExpression {
				alias,
				expression,
				fragment,
			}) => Expression::Alias(AliasExpression {
				alias,
				expression: m.boxed(expression)?,
				fragment,
			}),
			Expression::Cast(CastExpression {
				fragment,
				expression,
				to,
			}) => Expression::Cast(CastExpression {
				fragment,
				expression: m.boxed(expression)?,
				to,
			}),
			Expression::Prefix(PrefixExpression {
				operator,
				expression,
				fragment,
			}) => Expression::Prefix(PrefixExpression {
				operator,
				expression: m.boxed(expression)?,
				fragment,
			}),
			Expression::IsVariant(IsVariantExpression {
				expression,
				namespace,
				sumtype_name,
				variant_name,
				tag,
				fragment,
			}) => Expression::IsVariant(IsVariantExpression {
				expression: m.boxed(expression)?,
				namespace,
				sumtype_name,
				variant_name,
				tag,
				fragment,
			}),
			Expression::FieldAccess(FieldAccessExpression {
				object,
				field,
				fragment,
			}) => Expression::FieldAccess(FieldAccessExpression {
				object: m.boxed(object)?,
				field,
				fragment,
			}),
			Expression::Add(e) => Expression::Add(AddExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Div(e) => Expression::Div(DivExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Rem(e) => Expression::Rem(RemExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Mul(e) => Expression::Mul(MulExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Sub(e) => Expression::Sub(SubExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::GreaterThan(e) => Expression::GreaterThan(GreaterThanExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::GreaterThanEqual(e) => Expression::GreaterThanEqual(GreaterThanEqExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::LessThan(e) => Expression::LessThan(LessThanExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::LessThanEqual(e) => Expression::LessThanEqual(LessThanEqExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Equal(e) => Expression::Equal(EqExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::NotEqual(e) => Expression::NotEqual(NotEqExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::And(e) => Expression::And(AndExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Or(e) => Expression::Or(OrExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Xor(e) => Expression::Xor(XorExpression {
				left: m.boxed(e.left)?,
				right: m.boxed(e.right)?,
				fragment: e.fragment,
			}),
			Expression::Between(e) => Expression::Between(BetweenExpression {
				value: m.boxed(e.value)?,
				lower: m.boxed(e.lower)?,
				upper: m.boxed(e.upper)?,
				fragment: e.fragment,
			}),
			Expression::In(e) => Expression::In(InExpression {
				value: m.boxed(e.value)?,
				list: m.boxed(e.list)?,
				negated: e.negated,
				fragment: e.fragment,
			}),
			Expression::Contains(e) => Expression::Contains(ContainsExpression {
				value: m.boxed(e.value)?,
				list: m.boxed(e.list)?,
				fragment: e.fragment,
			}),
			Expression::Call(CallExpression {
				func,
				args,
				fragment,
			}) => Expression::Call(CallExpression {
				func,
				args: m.all(args)?,
				fragment,
			}),
			Expression::Tuple(e) => Expression::Tuple(TupleExpression {
				expressions: m.all(e.expressions)?,
				fragment: e.fragment,
			}),
			Expression::List(e) => Expression::List(ListExpression {
				expressions: m.all(e.expressions)?,
				fragment: e.fragment,
			}),
			Expression::Map(e) => Expression::Map(MapExpression {
				expressions: m.all(e.expressions)?,
				fragment: e.fragment,
			}),
			Expression::Extend(e) => Expression::Extend(ExtendExpression {
				expressions: m.all(e.expressions)?,
				fragment: e.fragment,
			}),
			Expression::SumTypeConstructor(SumTypeConstructorExpression {
				namespace,
				sumtype_name,
				variant_name,
				columns,
				fragment,
			}) => Expression::SumTypeConstructor(SumTypeConstructorExpression {
				namespace,
				sumtype_name,
				variant_name,
				columns: columns
					.into_iter()
					.map(|(name, expr)| Ok((name, m.map(expr)?)))
					.collect::<Result<_>>()?,
				fragment,
			}),
			Expression::If(IfExpression {
				condition,
				then_expr,
				else_ifs,
				else_expr,
				fragment,
			}) => Expression::If(IfExpression {
				condition: m.boxed(condition)?,
				then_expr: m.boxed(then_expr)?,
				else_ifs: else_ifs
					.into_iter()
					.map(|e| {
						Ok(ElseIfExpression {
							condition: m.boxed(e.condition)?,
							then_expr: m.boxed(e.then_expr)?,
							fragment: e.fragment,
						})
					})
					.collect::<Result<_>>()?,
				else_expr: else_expr.map(|e| m.boxed(e)).transpose()?,
				fragment,
			}),
		};
		Ok(Transformed::new(expr, m.transformed, m.tnr))
	}
}

struct Children<F> {
	f: F,
	transformed: bool,
	tnr: TreeNodeRecursion,
}

impl<F: FnMut(Expression) -> Result<Transformed<Expression>>> Children<F> {
	fn map(&mut self, expr: Expression) -> Result<Expression> {
		if self.tnr == TreeNodeRecursion::Stop {
			return Ok(expr);
		}
		let result = (self.f)(expr)?;
		self.tnr = result.tnr;
		self.transformed |= result.transformed;
		Ok(result.data)
	}

	fn boxed(&mut self, expr: Box<Expression>) -> Result<Box<Expression>> {
		Ok(Box::new(self.map(*expr)?))
	}

	fn all(&mut self, exprs: Vec<Expression>) -> Result<Vec<Expression>> {
		exprs.into_iter().map(|e| self.map(e)).collect()
	}
}

fn children(expr: &Expression) -> Vec<&Expression> {
	match expr {
		Expression::AccessSource(_)
		| Expression::Constant(_)
		| Expression::Column(_)
		| Expression::Type(_)
		| Expression::Parameter(_)
		| Expression::Variable(_) => vec![],
		Expression::Alias(e) => vec![&e.expression],
		Expression::Cast(e) => vec![&e.expression],
		Expression::Prefix(e) => vec![&e.expression],
		Expression::IsVariant(e) => vec![&e.expression],
		Expression::FieldAccess(e) => vec![&e.object],
		Expression::Add(e) => vec![&e.left, &e.right],
		Expression::Div(e) => vec![&e.left, &e.right],
		Expression::Rem(e) => vec![&e.left, &e.right],
		Expression::Mul(e) => vec![&e.left, &e.right],
		Expression::Sub(e) => vec![&e.left, &e.right],
		Expression::GreaterThan(e) => vec![&e.left, &e.right],
		Expression::GreaterThanEqual(e) => vec![&e.left, &e.right],
		Expression::LessThan(e) => vec![&e.left, &e.right],
		Expression::LessThanEqual(e) => vec![&e.left, &e.right],
		Expression::Equal(e) => vec![&e.left, &e.right],
		Expression::NotEqual(e) => vec![&e.left, &e.right],
		Expression::And(e) => vec![&e.left, &e.right],
		Expression::Or(e) => vec![&e.left, &e.right],
		Expression::Xor(e) => vec![&e.left, &e.right],
		Expression::Between(e) => vec![&e.value, &e.lower, &e.upper],
		Expression::In(e) => vec![&e.value, &e.list],
		Expression::Contains(e) => vec![&e.value, &e.list],
		Expression::Call(e) => e.args.iter().collect(),
		Expression::Tuple(e) => e.expressions.iter().collect(),
		Expression::List(e) => e.expressions.iter().collect(),
		Expression::Map(e) => e.expressions.iter().collect(),
		Expression::Extend(e) => e.expressions.iter().collect(),
		Expression::SumTypeConstructor(e) => e.columns.iter().map(|(_, expr)| expr).collect(),
		Expression::If(e) => {
			let mut out: Vec<&Expression> = vec![&e.condition, &e.then_expr];
			for else_if in &e.else_ifs {
				out.push(&else_if.condition);
				out.push(&else_if.then_expr);
			}
			if let Some(else_expr) = &e.else_expr {
				out.push(else_expr);
			}
			out
		}
	}
}
