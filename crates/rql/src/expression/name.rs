// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_core::interface::identifier::ColumnPrimitive;
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
			ConstantExpression::None {
				..
			} => Fragment::internal("none"),
		},
		Expression::AccessSource(access_expr) => {
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
		Expression::List(list_expr) => Fragment::internal(format!(
			"[{}]",
			list_expr
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
		Expression::Contains(c) => Fragment::internal(format!(
			"{} CONTAINS {}",
			simplified_name(&c.value).text(),
			simplified_name(&c.list).text()
		)),
		Expression::SumTypeConstructor(ctor) => {
			Fragment::internal(format!("{}::{}", ctor.sumtype_name.text(), ctor.variant_name.text()))
		}
		Expression::IsVariant(e) => Fragment::internal(format!(
			"{} IS {}{}::{}",
			simplified_name(&e.expression).text(),
			match &e.namespace {
				Some(ns) => format!("{}.", ns.text()),
				None => String::new(),
			},
			e.sumtype_name.text(),
			e.variant_name.text()
		)),
		Expression::FieldAccess(fa) => {
			Fragment::internal(format!("{}.{}", simplified_name(&fa.object).text(), fa.field.text()))
		}
	}
}

/// Recursively collect all column names referenced in an expression.
pub fn collect_column_names(expr: &Expression, names: &mut HashSet<String>) {
	match expr {
		Expression::Column(col) => {
			names.insert(col.0.name.text().to_string());
		}
		Expression::AccessSource(a) => {
			names.insert(a.column.name.text().to_string());
		}
		Expression::Alias(a) => collect_column_names(&a.expression, names),
		Expression::Call(c) => {
			for arg in &c.args {
				collect_column_names(arg, names);
			}
		}
		Expression::Cast(c) => collect_column_names(&c.expression, names),
		Expression::Prefix(p) => collect_column_names(&p.expression, names),
		Expression::Add(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Sub(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Mul(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Div(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Rem(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::GreaterThan(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::GreaterThanEqual(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::LessThan(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::LessThanEqual(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Equal(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::NotEqual(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::And(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Or(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Xor(e) => {
			collect_column_names(&e.left, names);
			collect_column_names(&e.right, names);
		}
		Expression::Between(b) => {
			collect_column_names(&b.value, names);
			collect_column_names(&b.lower, names);
			collect_column_names(&b.upper, names);
		}
		Expression::In(i) => {
			collect_column_names(&i.value, names);
			collect_column_names(&i.list, names);
		}
		Expression::Contains(c) => {
			collect_column_names(&c.value, names);
			collect_column_names(&c.list, names);
		}
		Expression::If(i) => {
			collect_column_names(&i.condition, names);
			collect_column_names(&i.then_expr, names);
			for else_if in &i.else_ifs {
				collect_column_names(&else_if.condition, names);
				collect_column_names(&else_if.then_expr, names);
			}
			if let Some(else_expr) = &i.else_expr {
				collect_column_names(else_expr, names);
			}
		}
		Expression::Tuple(t) => {
			for e in &t.expressions {
				collect_column_names(e, names);
			}
		}
		Expression::List(l) => {
			for e in &l.expressions {
				collect_column_names(e, names);
			}
		}
		Expression::Map(m) => {
			for e in &m.expressions {
				collect_column_names(e, names);
			}
		}
		Expression::Extend(ext) => {
			for e in &ext.expressions {
				collect_column_names(e, names);
			}
		}
		Expression::SumTypeConstructor(s) => {
			for (_, e) in &s.columns {
				collect_column_names(e, names);
			}
		}
		Expression::IsVariant(v) => collect_column_names(&v.expression, names),
		Expression::FieldAccess(f) => collect_column_names(&f.object, names),
		Expression::Constant(_) | Expression::Type(_) | Expression::Parameter(_) | Expression::Variable(_) => {}
	}
}

/// Collect all column names referenced across a slice of expressions.
pub fn collect_all_column_names(expressions: &[Expression]) -> HashSet<String> {
	let mut names = HashSet::new();
	for expr in expressions {
		collect_column_names(expr, &mut names);
	}
	names
}

#[cfg(test)]
mod tests {
	use std::{collections::HashSet, sync::Arc};

	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnPrimitive};
	use reifydb_type::{fragment::Fragment, value::r#type::Type};

	use super::{collect_all_column_names, collect_column_names};
	use crate::expression::{
		AccessPrimitiveExpression, AddExpression, AliasExpression, BetweenExpression, CallExpression,
		CastExpression, ColumnExpression, ConstantExpression, ElseIfExpression, Expression,
		FieldAccessExpression, IdentExpression, IfExpression, InExpression, IsVariantExpression,
		ListExpression, ParameterExpression, PrefixExpression, PrefixOperator, SumTypeConstructorExpression,
		TupleExpression, TypeExpression, VariableExpression,
	};

	fn frag(text: &str) -> Fragment {
		Fragment::Internal {
			text: Arc::from(text),
		}
	}

	fn col(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			primitive: ColumnPrimitive::Primitive {
				namespace: frag("ns"),
				primitive: frag("tbl"),
			},
			name: frag(name),
		}))
	}

	fn num(val: &str) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: frag(val),
		})
	}

	fn collect(expr: &Expression) -> HashSet<String> {
		let mut names = HashSet::new();
		collect_column_names(expr, &mut names);
		names
	}

	#[test]
	fn column_collects_name() {
		let result = collect(&col("age"));
		assert_eq!(result, HashSet::from(["age".to_string()]));
	}

	#[test]
	fn access_source_collects_name() {
		let expr = Expression::AccessSource(AccessPrimitiveExpression {
			column: ColumnIdentifier {
				primitive: ColumnPrimitive::Primitive {
					namespace: frag("ns"),
					primitive: frag("tbl"),
				},
				name: frag("salary"),
			},
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["salary".to_string()]));
	}

	#[test]
	fn leaf_nodes_produce_nothing() {
		assert!(collect(&num("42")).is_empty());

		let var = Expression::Variable(VariableExpression {
			fragment: frag("$x"),
		});
		assert!(collect(&var).is_empty());

		let param = Expression::Parameter(ParameterExpression::Positional {
			fragment: frag("$1"),
		});
		assert!(collect(&param).is_empty());

		let ty = Expression::Type(TypeExpression {
			fragment: frag("Int4"),
			ty: Type::Int4,
		});
		assert!(collect(&ty).is_empty());
	}

	#[test]
	fn binary_op_collects_from_both_sides() {
		let expr = Expression::Add(AddExpression {
			left: Box::new(col("a")),
			right: Box::new(col("b")),
			fragment: frag("+"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["a".to_string(), "b".to_string()]));
	}

	#[test]
	fn alias_collects_from_inner() {
		let expr = Expression::Alias(AliasExpression {
			alias: IdentExpression(frag("my_alias")),
			expression: Box::new(col("x")),
			fragment: frag("as"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["x".to_string()]));
	}

	#[test]
	fn cast_collects_from_inner() {
		let expr = Expression::Cast(CastExpression {
			fragment: frag("cast"),
			expression: Box::new(col("y")),
			to: TypeExpression {
				fragment: frag("Int4"),
				ty: Type::Int4,
			},
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["y".to_string()]));
	}

	#[test]
	fn prefix_collects_from_inner() {
		let expr = Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Minus(frag("-")),
			expression: Box::new(col("z")),
			fragment: frag("-"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["z".to_string()]));
	}

	#[test]
	fn tuple_collects_from_all_elements() {
		let expr = Expression::Tuple(TupleExpression {
			expressions: vec![col("a"), col("b"), num("1")],
			fragment: frag("()"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["a".to_string(), "b".to_string()]));
	}

	#[test]
	fn call_collects_from_args() {
		let expr = Expression::Call(CallExpression {
			func: IdentExpression(frag("sum")),
			args: vec![col("price"), col("qty")],
			fragment: frag("sum()"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["price".to_string(), "qty".to_string()]));
	}

	#[test]
	fn between_collects_from_value_lower_upper() {
		let expr = Expression::Between(BetweenExpression {
			value: Box::new(col("age")),
			lower: Box::new(col("min_age")),
			upper: Box::new(col("max_age")),
			fragment: frag("BETWEEN"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["age".to_string(), "min_age".to_string(), "max_age".to_string()]));
	}

	#[test]
	fn if_collects_from_all_branches() {
		let expr = Expression::If(IfExpression {
			condition: Box::new(col("cond")),
			then_expr: Box::new(col("then_col")),
			else_ifs: vec![ElseIfExpression {
				condition: Box::new(col("elif_cond")),
				then_expr: Box::new(col("elif_then")),
				fragment: frag("else if"),
			}],
			else_expr: Some(Box::new(col("else_col"))),
			fragment: frag("if"),
		});
		let result = collect(&expr);
		assert_eq!(
			result,
			HashSet::from([
				"cond".to_string(),
				"then_col".to_string(),
				"elif_cond".to_string(),
				"elif_then".to_string(),
				"else_col".to_string(),
			])
		);
	}

	#[test]
	fn in_collects_from_value_and_list() {
		let expr = Expression::In(InExpression {
			value: Box::new(col("status")),
			list: Box::new(Expression::List(ListExpression {
				expressions: vec![col("s1"), col("s2")],
				fragment: frag("[]"),
			})),
			negated: false,
			fragment: frag("IN"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["status".to_string(), "s1".to_string(), "s2".to_string()]));
	}

	#[test]
	fn sum_type_constructor_and_is_variant() {
		let ctor = Expression::SumTypeConstructor(SumTypeConstructorExpression {
			namespace: frag("ns"),
			sumtype_name: frag("Status"),
			variant_name: frag("Active"),
			columns: vec![(frag("val"), col("amount"))],
			fragment: frag("Status::Active"),
		});
		assert_eq!(collect(&ctor), HashSet::from(["amount".to_string()]));

		let is_v = Expression::IsVariant(IsVariantExpression {
			expression: Box::new(col("my_col")),
			namespace: None,
			sumtype_name: frag("Status"),
			variant_name: frag("Active"),
			tag: None,
			fragment: frag("IS"),
		});
		assert_eq!(collect(&is_v), HashSet::from(["my_col".to_string()]));
	}

	#[test]
	fn field_access_collects_from_object() {
		let expr = Expression::FieldAccess(FieldAccessExpression {
			object: Box::new(col("record")),
			field: frag("name"),
			fragment: frag("."),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["record".to_string()]));
	}

	#[test]
	fn nested_expression_deduplicates() {
		// a + a + b => {"a", "b"}
		let expr = Expression::Add(AddExpression {
			left: Box::new(Expression::Add(AddExpression {
				left: Box::new(col("a")),
				right: Box::new(col("a")),
				fragment: frag("+"),
			})),
			right: Box::new(col("b")),
			fragment: frag("+"),
		});
		let result = collect(&expr);
		assert_eq!(result, HashSet::from(["a".to_string(), "b".to_string()]));
	}

	#[test]
	fn collect_all_column_names_across_expressions() {
		let exprs = vec![col("x"), col("y"), num("1"), col("x")];
		let result = collect_all_column_names(&exprs);
		assert_eq!(result, HashSet::from(["x".to_string(), "y".to_string()]));
	}
}
