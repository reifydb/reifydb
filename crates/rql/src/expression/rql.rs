// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Serialize `Expression` → valid RQL text for remote push-down.
//!
//! Returns `None` when a recursive sub-expression cannot be serialized.

use reifydb_core::{interface::identifier::ColumnPrimitive, sort::SortKey};

use super::{
	AccessPrimitiveExpression, AndExpression, ColumnExpression, ConstantExpression, Expression, OrExpression,
	ParameterExpression, PrefixExpression, PrefixOperator, XorExpression,
};

pub fn expression_to_rql(expr: &Expression) -> Option<String> {
	match expr {
		Expression::Constant(c) => Some(constant_to_rql(c)),
		Expression::Column(ColumnExpression(col)) => Some(col.name.text().to_string()),

		// Arithmetic
		Expression::Add(e) => binary_to_rql(&e.left, "+", &e.right),
		Expression::Sub(e) => binary_to_rql(&e.left, "-", &e.right),
		Expression::Mul(e) => binary_to_rql(&e.left, "*", &e.right),
		Expression::Div(e) => binary_to_rql(&e.left, "/", &e.right),
		Expression::Rem(e) => binary_to_rql(&e.left, "%", &e.right),

		// Comparison
		Expression::GreaterThan(e) => binary_to_rql(&e.left, ">", &e.right),
		Expression::GreaterThanEqual(e) => binary_to_rql(&e.left, ">=", &e.right),
		Expression::LessThan(e) => binary_to_rql(&e.left, "<", &e.right),
		Expression::LessThanEqual(e) => binary_to_rql(&e.left, "<=", &e.right),
		Expression::Equal(e) => binary_to_rql(&e.left, "==", &e.right),
		Expression::NotEqual(e) => binary_to_rql(&e.left, "!=", &e.right),

		// Logical
		Expression::And(AndExpression {
			left,
			right,
			..
		}) => binary_to_rql(left, "and", right),
		Expression::Or(OrExpression {
			left,
			right,
			..
		}) => binary_to_rql(left, "or", right),
		Expression::Xor(XorExpression {
			left,
			right,
			..
		}) => binary_to_rql(left, "xor", right),

		// Between
		Expression::Between(e) => {
			let v = expression_to_rql(&e.value)?;
			let lo = expression_to_rql(&e.lower)?;
			let hi = expression_to_rql(&e.upper)?;
			Some(format!("({} BETWEEN {} AND {})", v, lo, hi))
		}

		// In / Not In
		Expression::In(e) => {
			let val = expression_to_rql(&e.value)?;
			let list = tuple_as_brackets(&e.list)?;
			if e.negated {
				Some(format!("({} NOT IN {})", val, list))
			} else {
				Some(format!("({} IN {})", val, list))
			}
		}

		// Contains
		Expression::Contains(e) => {
			let val = expression_to_rql(&e.value)?;
			let list = tuple_as_brackets(&e.list)?;
			Some(format!("({} CONTAINS {})", val, list))
		}

		// Prefix operators (not, -, +)
		Expression::Prefix(PrefixExpression {
			operator,
			expression,
			..
		}) => {
			let inner = expression_to_rql(expression)?;
			let op = match operator {
				PrefixOperator::Minus(_) => "-",
				PrefixOperator::Plus(_) => "+",
				PrefixOperator::Not(_) => "not ",
			};
			Some(format!("({}{})", op, inner))
		}

		// Tuple (used for IN lists)
		Expression::Tuple(t) => {
			let items: Option<Vec<String>> = t.expressions.iter().map(expression_to_rql).collect();
			Some(format!("({})", items?.join(", ")))
		}

		// List
		Expression::List(l) => {
			let items: Option<Vec<String>> = l.expressions.iter().map(expression_to_rql).collect();
			Some(format!("[{}]", items?.join(", ")))
		}

		// Cast — passthrough the inner expression
		Expression::Cast(e) => expression_to_rql(&e.expression),

		// Alias — use the inner expression
		Expression::Alias(e) => {
			let inner = expression_to_rql(&e.expression)?;
			Some(format!("{} as {}", inner, e.alias.0.text()))
		}

		// Tier 1 — trivial (no recursion)
		Expression::Parameter(param) => match param {
			ParameterExpression::Positional {
				fragment,
			} => Some(fragment.text().to_string()),
			ParameterExpression::Named {
				fragment,
			} => Some(fragment.text().to_string()),
		},
		Expression::Variable(var) => Some(var.fragment.text().to_string()),
		Expression::AccessSource(AccessPrimitiveExpression {
			column,
		}) => match &column.primitive {
			ColumnPrimitive::Primitive {
				primitive,
				..
			} => Some(format!("{}.{}", primitive.text(), column.name.text())),
			ColumnPrimitive::Alias(alias) => Some(format!("{}.{}", alias.text(), column.name.text())),
		},
		Expression::Type(t) => Some(t.fragment.text().to_string()),

		// Tier 2 — recursive on sub-expressions
		Expression::FieldAccess(fa) => {
			let obj = expression_to_rql(&fa.object)?;
			Some(format!("{}.{}", obj, fa.field.text()))
		}
		Expression::Call(call) => {
			let args: Option<Vec<String>> = call.args.iter().map(expression_to_rql).collect();
			Some(format!("{}({})", call.func.0.text(), args?.join(", ")))
		}
		Expression::IsVariant(e) => {
			let inner = expression_to_rql(&e.expression)?;
			let qualified = match &e.namespace {
				Some(ns) => {
					format!("{}::{}::{}", ns.text(), e.sumtype_name.text(), e.variant_name.text())
				}
				None => format!("{}::{}", e.sumtype_name.text(), e.variant_name.text()),
			};
			Some(format!("({} IS {})", inner, qualified))
		}

		// Tier 3 — multi-part recursive
		Expression::If(if_expr) => {
			let cond = expression_to_rql(&if_expr.condition)?;
			let then = expression_to_rql(&if_expr.then_expr)?;
			let mut result = format!("if {} {{ {} }}", cond, then);
			for else_if in &if_expr.else_ifs {
				let c = expression_to_rql(&else_if.condition)?;
				let t = expression_to_rql(&else_if.then_expr)?;
				result.push_str(&format!(" else if {} {{ {} }}", c, t));
			}
			if let Some(ref else_expr) = if_expr.else_expr {
				let e = expression_to_rql(else_expr)?;
				result.push_str(&format!(" else {{ {} }}", e));
			}
			Some(result)
		}
		Expression::Map(map_expr) => {
			let items: Option<Vec<String>> = map_expr.expressions.iter().map(expression_to_rql).collect();
			Some(format!("MAP{{ {} }}", items?.join(", ")))
		}
		Expression::Extend(ext) => {
			let items: Option<Vec<String>> = ext.expressions.iter().map(expression_to_rql).collect();
			Some(format!("EXTEND{{ {} }}", items?.join(", ")))
		}
		Expression::SumTypeConstructor(ctor) => {
			let fields: Option<Vec<String>> = ctor
				.columns
				.iter()
				.map(|(name, expr)| {
					let e = expression_to_rql(expr)?;
					Some(format!("{}: {}", name.text(), e))
				})
				.collect();
			Some(format!(
				"{}::{}{{ {} }}",
				ctor.sumtype_name.text(),
				ctor.variant_name.text(),
				fields?.join(", ")
			))
		}
	}
}

/// Serialize an expression for use within EXTEND { ... } context.
/// Alias expressions use colon syntax (name: expr) instead of (expr as name).
pub fn extend_expression_to_rql(expr: &Expression) -> Option<String> {
	if let Expression::Alias(e) = expr {
		let inner = expression_to_rql(&e.expression)?;
		Some(format!("{}: {}", e.alias.0.text(), inner))
	} else {
		expression_to_rql(expr)
	}
}

fn tuple_as_brackets(expr: &Expression) -> Option<String> {
	if let Expression::Tuple(t) = expr {
		let items: Option<Vec<String>> = t.expressions.iter().map(expression_to_rql).collect();
		Some(format!("[{}]", items?.join(", ")))
	} else {
		expression_to_rql(expr)
	}
}

fn binary_to_rql(left: &Expression, op: &str, right: &Expression) -> Option<String> {
	let l = expression_to_rql(left)?;
	let r = expression_to_rql(right)?;
	Some(format!("({} {} {})", l, op, r))
}

fn constant_to_rql(c: &ConstantExpression) -> String {
	match c {
		ConstantExpression::None {
			..
		} => "none".to_string(),
		ConstantExpression::Bool {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Number {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Text {
			fragment,
		} => format!("'{}'", fragment.text()),
		ConstantExpression::Temporal {
			fragment,
		} => format!("d'{}'", fragment.text()),
	}
}

pub fn sort_key_to_rql(key: &SortKey) -> String {
	format!("{}: {}", key.column.fragment(), key.direction)
}
