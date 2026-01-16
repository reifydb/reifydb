// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::fragment::Fragment;

use crate::expression::{ConstantExpression, Expression};

/// Get the column name for an expression
pub fn column_name_from_expression<'a>(expr: &Expression) -> Fragment {
	match expr {
		Expression::Alias(alias_expr) => alias_expr.alias.0.clone(),
		Expression::Column(col_expr) => col_expr.0.name.clone(),
		Expression::AccessSource(access_expr) => access_expr.column.name.clone(),
		_ => simplified_name(expr),
	}
}

fn simplified_name<'a>(expr: &Expression) -> Fragment {
	match expr {
		Expression::Add(expr) => Fragment::internal(format!(
			"{}+{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Sub(expr) => Fragment::internal(format!(
			"{}-{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Mul(expr) => Fragment::internal(format!(
			"{}*{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Div(expr) => Fragment::internal(format!(
			"{}/{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Rem(expr) => Fragment::internal(format!(
			"{}%{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Column(col_expr) => col_expr.0.name.clone(),
		Expression::Constant(const_expr) => match const_expr {
			ConstantExpression::Number {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Text {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Bool {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Temporal {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Undefined {
				..
			} => Fragment::internal("undefined"),
		},
		Expression::AccessSource(access_expr) => {
			use reifydb_core::interface::identifier::ColumnPrimitive;

			// Extract primitive name based on the ColumnPrimitive type
			let primitive_name = match &access_expr.column.primitive {
				ColumnPrimitive::Primitive {
					primitive,
					..
				} => primitive.text(),
				ColumnPrimitive::Alias(alias) => alias.text(),
			};

			Fragment::internal(format!("{}.{}", primitive_name, access_expr.column.name.text()))
		}
		Expression::Call(call_expr) => Fragment::internal(format!(
			"{}({})",
			call_expr.func.name(),
			call_expr
				.args
				.iter()
				.map(|arg| simplified_name(arg).text().to_string())
				.collect::<Vec<_>>()
				.join(",")
		)),
		Expression::Prefix(prefix_expr) => Fragment::internal(format!(
			"{}{}",
			prefix_expr.operator,
			simplified_name(&prefix_expr.expression).text()
		)),
		Expression::Cast(cast_expr) => simplified_name(&cast_expr.expression),
		Expression::Alias(alias_expr) => Fragment::internal(alias_expr.alias.name()),
		Expression::Tuple(tuple_expr) => Fragment::internal(format!(
			"({})",
			tuple_expr
				.expressions
				.iter()
				.map(|e| simplified_name(e).text().to_string())
				.collect::<Vec<_>>()
				.join(",")
		)),
		Expression::GreaterThan(expr) => Fragment::internal(format!(
			"{}>{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::GreaterThanEqual(expr) => Fragment::internal(format!(
			"{}>={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::LessThan(expr) => Fragment::internal(format!(
			"{}<{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::LessThanEqual(expr) => Fragment::internal(format!(
			"{}<={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Equal(expr) => Fragment::internal(format!(
			"{}=={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::NotEqual(expr) => Fragment::internal(format!(
			"{}!={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Between(expr) => Fragment::internal(format!(
			"{} BETWEEN {} AND {}",
			simplified_name(&expr.value).text(),
			simplified_name(&expr.lower).text(),
			simplified_name(&expr.upper).text()
		)),
		Expression::And(expr) => Fragment::internal(format!(
			"{}and{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Or(expr) => Fragment::internal(format!(
			"{}or{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Xor(expr) => Fragment::internal(format!(
			"{}xor{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Type(type_expr) => type_expr.fragment.clone(),
		Expression::Parameter(_) => Fragment::internal("parameter"),
		Expression::Variable(var) => Fragment::internal(format!("var_{}", var.name())),
		Expression::If(if_expr) => Fragment::internal(format!(
			"if({},{}{})",
			simplified_name(&if_expr.condition).text(),
			simplified_name(&if_expr.then_expr).text(),
			if let Some(else_expr) = &if_expr.else_expr {
				format!(",{}", simplified_name(else_expr).text())
			} else {
				String::new()
			}
		)),
		Expression::Map(_map_expr) => Fragment::internal("map"),
		Expression::Extend(_extend_expr) => Fragment::internal("extend"),
		Expression::In(in_expr) => Fragment::internal(format!(
			"{} IN {}",
			simplified_name(&in_expr.value).text(),
			simplified_name(&in_expr.list).text()
		)),
	}
}
