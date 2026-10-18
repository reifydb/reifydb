// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use reifydb_core::{
	error::diagnostic::catalog::{variant_enum_not_known, variant_in_expression},
	expression::{Expression, IsVariantExpression, name::display_label},
	value::column::factory,
};
use reifydb_value::{
	error,
	error::Error,
	value::{
		system_columns::resolve_column,
		value_type::{ValueType, field::from_field},
	},
};

use crate::{
	Result,
	expression::context::EvalContext,
	lower::{Node, bool_field, literal, schema::positional},
};

pub(super) fn compile_time_error(expression: &Expression) -> Option<Error> {
	match expression {
		Expression::SumTypeConstructor(constructor) => {
			return Some(error!(variant_in_expression(constructor.variant_name.clone())));
		}
		Expression::IsVariant(e) if e.tag.is_none() => {
			return Some(error!(variant_enum_not_known(e.variant_name.clone(), &variant_column(e))));
		}
		Expression::IsVariant(_) => return None,
		_ => {}
	}
	children(expression).into_iter().find_map(compile_time_error)
}

pub(super) fn children(expression: &Expression) -> Vec<&Expression> {
	match expression {
		Expression::Alias(e) => vec![&e.expression],
		Expression::Cast(e) => vec![&e.expression],
		Expression::Prefix(e) => vec![&e.expression],
		Expression::Add(e) => vec![&e.left, &e.right],
		Expression::Sub(e) => vec![&e.left, &e.right],
		Expression::Mul(e) => vec![&e.left, &e.right],
		Expression::Div(e) => vec![&e.left, &e.right],
		Expression::Rem(e) => vec![&e.left, &e.right],
		Expression::Equal(e) => vec![&e.left, &e.right],
		Expression::NotEqual(e) => vec![&e.left, &e.right],
		Expression::LessThan(e) => vec![&e.left, &e.right],
		Expression::LessThanEqual(e) => vec![&e.left, &e.right],
		Expression::GreaterThan(e) => vec![&e.left, &e.right],
		Expression::GreaterThanEqual(e) => vec![&e.left, &e.right],
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
		Expression::FieldAccess(e) => vec![&e.object],
		Expression::If(e) => {
			let mut all: Vec<&Expression> = vec![&e.condition, &e.then_expr];
			for else_if in &e.else_ifs {
				all.push(&else_if.condition);
				all.push(&else_if.then_expr);
			}
			all.extend(e.else_expr.as_deref());
			all
		}
		Expression::AccessSource(_)
		| Expression::Column(_)
		| Expression::Constant(_)
		| Expression::Type(_)
		| Expression::Parameter(_)
		| Expression::Variable(_)
		| Expression::SumTypeConstructor(_)
		| Expression::IsVariant(_) => vec![],
	}
}

fn variant_column(e: &IsVariantExpression) -> String {
	match e.expression.as_ref() {
		Expression::Column(c) => c.0.name.text().to_string(),
		other => display_label(other).text().to_string(),
	}
}

pub(super) fn lower_is_variant(ctx: &EvalContext, operator: &str, e: &IsVariantExpression) -> Result<Node> {
	let column = variant_column(e);
	let Some(tag) = e.tag else {
		return Err(error!(variant_enum_not_known(e.variant_name.clone(), &column)));
	};
	let name = e.fragment.text();
	if let Some(index) = resolve_column(&ctx.batch, &format!("{column}_tag")) {
		let field = &ctx.batch.schema_ref().fields()[index];
		if !field.is_nullable() && from_field(field)?.value_type == Some(ValueType::Uint1) {
			return Ok(Node {
				expr: Expr::BinaryExpr(BinaryExpr::new(
					Box::new(positional(index)),
					Operator::Eq,
					Box::new(Expr::Literal(ScalarValue::UInt8(Some(tag)), None)),
				)),
				field: bool_field(name, false),
			});
		}
	}
	let (field, array) = factory::none_typed(name, ValueType::Boolean, 1);
	literal::row_zero(operator, &field, &array)
}
