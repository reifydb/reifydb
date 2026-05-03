// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_core::interface::identifier::ColumnShape;
use reifydb_type::fragment::Fragment;

use crate::expression::{AccessShapeExpression, ConstantExpression, Expression, ParameterExpression, PrefixOperator};

pub fn canonical_name(expr: &Expression) -> Fragment {
	Fragment::internal(canonical_text(expr))
}

pub fn display_label(expr: &Expression) -> Fragment {
	match expr {
		Expression::Alias(alias_expr) => alias_expr.alias.0.clone(),
		Expression::Column(col_expr) => col_expr.0.name.clone(),
		Expression::AccessSource(access_expr) => access_source_name(access_expr),
		Expression::Constant(const_expr) => constant_label(const_expr),
		Expression::Variable(var) => var.fragment.clone(),
		Expression::Parameter(param) => match param {
			ParameterExpression::Positional {
				fragment,
			}
			| ParameterExpression::Named {
				fragment,
			} => fragment.clone(),
		},
		Expression::Type(t) => t.fragment.clone(),
		_ => canonical_name(expr),
	}
}

#[deprecated(note = "use display_label for header naming or canonical_name for identity")]
pub fn column_name_from_expression(expr: &Expression) -> Fragment {
	display_label(expr)
}

fn canonical_text(expr: &Expression) -> String {
	match expr {
		Expression::Alias(alias_expr) => canonical_text(&alias_expr.expression),

		Expression::Column(col_expr) => col_expr.0.name.text().to_string(),

		Expression::AccessSource(access_expr) => access_source_name(access_expr).text().to_string(),

		Expression::Constant(const_expr) => constant_canonical(const_expr),

		Expression::Variable(var) => var.fragment.text().to_string(),

		Expression::Parameter(param) => match param {
			ParameterExpression::Positional {
				fragment,
			}
			| ParameterExpression::Named {
				fragment,
			} => fragment.text().to_string(),
		},

		Expression::Type(t) => t.fragment.text().to_string(),

		Expression::Add(e) => binary(expr, &e.left, "+", &e.right),
		Expression::Sub(e) => binary(expr, &e.left, "-", &e.right),
		Expression::Mul(e) => binary(expr, &e.left, "*", &e.right),
		Expression::Div(e) => binary(expr, &e.left, "/", &e.right),
		Expression::Rem(e) => binary(expr, &e.left, "%", &e.right),

		Expression::GreaterThan(e) => binary(expr, &e.left, ">", &e.right),
		Expression::GreaterThanEqual(e) => binary(expr, &e.left, ">=", &e.right),
		Expression::LessThan(e) => binary(expr, &e.left, "<", &e.right),
		Expression::LessThanEqual(e) => binary(expr, &e.left, "<=", &e.right),
		Expression::Equal(e) => binary(expr, &e.left, "==", &e.right),
		Expression::NotEqual(e) => binary(expr, &e.left, "!=", &e.right),

		Expression::And(e) => binary(expr, &e.left, "and", &e.right),
		Expression::Or(e) => binary(expr, &e.left, "or", &e.right),
		Expression::Xor(e) => binary(expr, &e.left, "xor", &e.right),

		Expression::Between(e) => format!(
			"{} between {} and {}",
			child_text(expr, &e.value),
			child_text(expr, &e.lower),
			child_text(expr, &e.upper)
		),

		Expression::In(e) => {
			let kw = if e.negated {
				"not in"
			} else {
				"in"
			};
			format!("{} {} {}", child_text(expr, &e.value), kw, child_text(expr, &e.list))
		}

		Expression::Contains(e) => {
			format!("{} contains {}", child_text(expr, &e.value), child_text(expr, &e.list))
		}

		Expression::IsVariant(e) => {
			let ns = match &e.namespace {
				Some(n) => format!("{}.", n.text()),
				None => String::new(),
			};
			format!(
				"{} is {}{}::{}",
				child_text(expr, &e.expression),
				ns,
				e.sumtype_name.text(),
				e.variant_name.text()
			)
		}

		Expression::Prefix(e) => {
			let op = match &e.operator {
				PrefixOperator::Minus(_) => "-",
				PrefixOperator::Plus(_) => "+",
				PrefixOperator::Not(_) => "not",
			};
			let inner = child_text(expr, &e.expression);
			match e.operator {
				PrefixOperator::Not(_) => format!("{} {}", op, inner),
				_ => format!("{}{}", op, inner),
			}
		}

		Expression::Cast(e) => {
			format!("cast({}, {})", canonical_text(&e.expression), e.to.fragment.text())
		}

		Expression::Call(call) => {
			let args = call.args.iter().map(canonical_text).collect::<Vec<_>>().join(", ");
			format!("{}({})", call.func.0.text(), args)
		}

		Expression::Tuple(t) => {
			let items = t.expressions.iter().map(canonical_text).collect::<Vec<_>>().join(", ");
			format!("({})", items)
		}

		Expression::List(l) => {
			let items = l.expressions.iter().map(canonical_text).collect::<Vec<_>>().join(", ");
			format!("[{}]", items)
		}

		Expression::Map(m) => {
			let items = m.expressions.iter().map(canonical_text).collect::<Vec<_>>().join(", ");
			format!("map({})", items)
		}

		Expression::Extend(e) => {
			let items = e.expressions.iter().map(canonical_text).collect::<Vec<_>>().join(", ");
			format!("extend({})", items)
		}

		Expression::SumTypeConstructor(ctor) => {
			let fields = ctor
				.columns
				.iter()
				.map(|(name, value)| format!("{}: {}", name.text(), canonical_text(value)))
				.collect::<Vec<_>>()
				.join(", ");
			format!("{}::{}({})", ctor.sumtype_name.text(), ctor.variant_name.text(), fields)
		}

		Expression::FieldAccess(fa) => {
			format!("{}.{}", canonical_text(&fa.object), fa.field.text())
		}

		Expression::If(if_expr) => {
			let mut s = format!(
				"if {} then {}",
				canonical_text(&if_expr.condition),
				canonical_text(&if_expr.then_expr)
			);
			for else_if in &if_expr.else_ifs {
				s.push_str(&format!(
					" else if {} then {}",
					canonical_text(&else_if.condition),
					canonical_text(&else_if.then_expr)
				));
			}
			if let Some(else_expr) = &if_expr.else_expr {
				s.push_str(&format!(" else {}", canonical_text(else_expr)));
			}
			s
		}
	}
}

fn binary(parent: &Expression, left: &Expression, op: &str, right: &Expression) -> String {
	format!("{} {} {}", child_text(parent, left), op, child_text(parent, right))
}

fn child_text(parent: &Expression, child: &Expression) -> String {
	let inner = canonical_text(child);
	if needs_parens(parent, child) {
		format!("({})", inner)
	} else {
		inner
	}
}

fn needs_parens(parent: &Expression, child: &Expression) -> bool {
	let p_prec = precedence(parent);
	let c_prec = precedence(child);
	c_prec < p_prec
}

fn precedence(expr: &Expression) -> u8 {
	match expr {
		Expression::Or(_) | Expression::Xor(_) => 1,
		Expression::And(_) => 2,
		Expression::Equal(_)
		| Expression::NotEqual(_)
		| Expression::GreaterThan(_)
		| Expression::GreaterThanEqual(_)
		| Expression::LessThan(_)
		| Expression::LessThanEqual(_)
		| Expression::Between(_)
		| Expression::In(_)
		| Expression::Contains(_)
		| Expression::IsVariant(_) => 3,
		Expression::Add(_) | Expression::Sub(_) => 4,
		Expression::Mul(_) | Expression::Div(_) | Expression::Rem(_) => 5,
		Expression::Prefix(_) => 6,
		Expression::Alias(a) => precedence(&a.expression),
		_ => 7,
	}
}

fn access_source_name(access_expr: &AccessShapeExpression) -> Fragment {
	let shape_name = match &access_expr.column.shape {
		ColumnShape::Qualified {
			name,
			..
		} => name.text(),
		ColumnShape::Alias(alias) => alias.text(),
	};
	Fragment::internal(format!("{}_{}", shape_name, access_expr.column.name.text()))
}

fn constant_canonical(c: &ConstantExpression) -> String {
	match c {
		ConstantExpression::None {
			..
		} => "none".to_string(),
		ConstantExpression::Bool {
			fragment,
		}
		| ConstantExpression::Number {
			fragment,
		}
		| ConstantExpression::Temporal {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Text {
			fragment,
		} => format!("\"{}\"", fragment.text()),
	}
}

fn constant_label(c: &ConstantExpression) -> Fragment {
	match c {
		ConstantExpression::None {
			..
		} => Fragment::internal("none"),
		ConstantExpression::Bool {
			fragment,
		}
		| ConstantExpression::Number {
			fragment,
		}
		| ConstantExpression::Temporal {
			fragment,
		} => fragment.clone(),
		ConstantExpression::Text {
			fragment,
		} => Fragment::internal(format!("\"{}\"", fragment.text())),
	}
}

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

	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnShape};
	use reifydb_type::{fragment::Fragment, value::r#type::Type};

	use super::{canonical_name, collect_all_column_names, collect_column_names, display_label};
	use crate::expression::{
		AccessShapeExpression, AddExpression, AliasExpression, AndExpression, BetweenExpression,
		CallExpression, CastExpression, ColumnExpression, ConstantExpression, ContainsExpression,
		ElseIfExpression, EqExpression, Expression, ExtendExpression, FieldAccessExpression,
		GreaterThanExpression, IdentExpression, IfExpression, InExpression, IsVariantExpression,
		LessThanExpression, ListExpression, MapExpression, MulExpression, NotEqExpression, OrExpression,
		ParameterExpression, PrefixExpression, PrefixOperator, SubExpression, SumTypeConstructorExpression,
		TupleExpression, TypeExpression, VariableExpression, XorExpression,
	};

	fn frag(text: &str) -> Fragment {
		Fragment::Internal {
			text: Arc::from(text),
		}
	}

	fn col(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			shape: ColumnShape::Qualified {
				namespace: frag("ns"),
				name: frag("tbl"),
			},
			name: frag(name),
		}))
	}

	fn num(val: &str) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: frag(val),
		})
	}

	fn text(val: &str) -> Expression {
		Expression::Constant(ConstantExpression::Text {
			fragment: frag(val),
		})
	}

	fn add(l: Expression, r: Expression) -> Expression {
		Expression::Add(AddExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("+"),
		})
	}

	fn sub(l: Expression, r: Expression) -> Expression {
		Expression::Sub(SubExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("-"),
		})
	}

	fn mul(l: Expression, r: Expression) -> Expression {
		Expression::Mul(MulExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("*"),
		})
	}

	fn and_e(l: Expression, r: Expression) -> Expression {
		Expression::And(AndExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("and"),
		})
	}

	fn or_e(l: Expression, r: Expression) -> Expression {
		Expression::Or(OrExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("or"),
		})
	}

	fn xor_e(l: Expression, r: Expression) -> Expression {
		Expression::Xor(XorExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("xor"),
		})
	}

	fn gt(l: Expression, r: Expression) -> Expression {
		Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag(">"),
		})
	}

	fn lt(l: Expression, r: Expression) -> Expression {
		Expression::LessThan(LessThanExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("<"),
		})
	}

	fn eq(l: Expression, r: Expression) -> Expression {
		Expression::Equal(EqExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("=="),
		})
	}

	fn neq(l: Expression, r: Expression) -> Expression {
		Expression::NotEqual(NotEqExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("!="),
		})
	}

	fn collect(expr: &Expression) -> HashSet<String> {
		let mut names = HashSet::new();
		collect_column_names(expr, &mut names);
		names
	}

	#[test]
	fn canonical_column() {
		assert_eq!(canonical_name(&col("age")).text(), "age");
	}

	#[test]
	fn canonical_constant_number() {
		assert_eq!(canonical_name(&num("42")).text(), "42");
	}

	#[test]
	fn canonical_constant_text_keeps_quotes() {
		assert_eq!(canonical_name(&text("hello")).text(), "\"hello\"");
	}

	#[test]
	fn canonical_constant_none() {
		let none = Expression::Constant(ConstantExpression::None {
			fragment: frag("none"),
		});
		assert_eq!(canonical_name(&none).text(), "none");
	}

	#[test]
	fn canonical_add() {
		assert_eq!(canonical_name(&add(col("a"), col("b"))).text(), "a + b");
	}

	#[test]
	fn canonical_sub() {
		assert_eq!(canonical_name(&sub(col("a"), col("b"))).text(), "a - b");
	}

	#[test]
	fn canonical_and_lowercase_spaced() {
		// Regression: was "aandb" with no spaces.
		assert_eq!(canonical_name(&and_e(col("a"), col("b"))).text(), "a and b");
	}

	#[test]
	fn canonical_or_lowercase_spaced() {
		assert_eq!(canonical_name(&or_e(col("a"), col("b"))).text(), "a or b");
	}

	#[test]
	fn canonical_xor_lowercase_spaced() {
		assert_eq!(canonical_name(&xor_e(col("a"), col("b"))).text(), "a xor b");
	}

	#[test]
	fn canonical_comparison_operators() {
		assert_eq!(canonical_name(&gt(col("a"), num("0"))).text(), "a > 0");
		assert_eq!(canonical_name(&lt(col("a"), num("0"))).text(), "a < 0");
		assert_eq!(canonical_name(&eq(col("a"), num("0"))).text(), "a == 0");
		assert_eq!(canonical_name(&neq(col("a"), num("0"))).text(), "a != 0");
	}

	#[test]
	fn canonical_precedence_no_parens_when_higher_child() {
		// a + b * c - mul has higher prec than add, no parens needed
		let e = add(col("a"), mul(col("b"), col("c")));
		assert_eq!(canonical_name(&e).text(), "a + b * c");
	}

	#[test]
	fn canonical_precedence_parens_when_lower_child() {
		// (a + b) * c - add has lower prec than mul, parens needed
		let e = mul(add(col("a"), col("b")), col("c"));
		assert_eq!(canonical_name(&e).text(), "(a + b) * c");
	}

	#[test]
	fn canonical_precedence_chained_and_or() {
		// a and b or c - or is lower than and, no parens around `a and b`
		let e = or_e(and_e(col("a"), col("b")), col("c"));
		assert_eq!(canonical_name(&e).text(), "a and b or c");
	}

	#[test]
	fn canonical_precedence_or_inside_and() {
		// a and (b or c) - or is lower than and, parens needed
		let e = and_e(col("a"), or_e(col("b"), col("c")));
		assert_eq!(canonical_name(&e).text(), "a and (b or c)");
	}

	#[test]
	fn canonical_between_symmetric() {
		let e = Expression::Between(BetweenExpression {
			value: Box::new(col("x")),
			lower: Box::new(num("1")),
			upper: Box::new(num("10")),
			fragment: frag("between"),
		});
		assert_eq!(canonical_name(&e).text(), "x between 1 and 10");
	}

	#[test]
	fn canonical_in_and_not_in() {
		let in_e = Expression::In(InExpression {
			value: Box::new(col("status")),
			list: Box::new(Expression::List(ListExpression {
				expressions: vec![text("a"), text("b")],
				fragment: frag("[]"),
			})),
			negated: false,
			fragment: frag("in"),
		});
		assert_eq!(canonical_name(&in_e).text(), "status in [\"a\", \"b\"]");

		let not_in = Expression::In(InExpression {
			value: Box::new(col("status")),
			list: Box::new(Expression::List(ListExpression {
				expressions: vec![text("a")],
				fragment: frag("[]"),
			})),
			negated: true,
			fragment: frag("in"),
		});
		assert_eq!(canonical_name(&not_in).text(), "status not in [\"a\"]");
	}

	#[test]
	fn canonical_contains() {
		let e = Expression::Contains(ContainsExpression {
			value: Box::new(col("tags")),
			list: Box::new(text("urgent")),
			fragment: frag("contains"),
		});
		assert_eq!(canonical_name(&e).text(), "tags contains \"urgent\"");
	}

	#[test]
	fn canonical_cast_preserves_type() {
		// Regression: was dropping the cast and returning just the inner name.
		let e = Expression::Cast(CastExpression {
			fragment: frag("cast"),
			expression: Box::new(col("y")),
			to: TypeExpression {
				fragment: frag("Int4"),
				ty: Type::Int4,
			},
		});
		assert_eq!(canonical_name(&e).text(), "cast(y, Int4)");
	}

	#[test]
	fn canonical_call_with_space_after_comma() {
		let e = Expression::Call(CallExpression {
			func: IdentExpression(frag("count")),
			args: vec![col("price"), col("qty")],
			fragment: frag("count()"),
		});
		assert_eq!(canonical_name(&e).text(), "count(price, qty)");
	}

	#[test]
	fn canonical_map_not_hardcoded() {
		// Regression: was hardcoded "map", losing inner expression detail.
		let e = Expression::Map(MapExpression {
			expressions: vec![col("a"), col("b")],
			fragment: frag("{}"),
		});
		assert_eq!(canonical_name(&e).text(), "map(a, b)");
	}

	#[test]
	fn canonical_extend_not_hardcoded() {
		let e = Expression::Extend(ExtendExpression {
			expressions: vec![col("x")],
			fragment: frag("{}"),
		});
		assert_eq!(canonical_name(&e).text(), "extend(x)");
	}

	#[test]
	fn canonical_tuple() {
		let e = Expression::Tuple(TupleExpression {
			expressions: vec![col("a"), num("1")],
			fragment: frag("()"),
		});
		assert_eq!(canonical_name(&e).text(), "(a, 1)");
	}

	#[test]
	fn canonical_list() {
		let e = Expression::List(ListExpression {
			expressions: vec![num("1"), num("2"), num("3")],
			fragment: frag("[]"),
		});
		assert_eq!(canonical_name(&e).text(), "[1, 2, 3]");
	}

	#[test]
	fn canonical_parameter_positional() {
		// Regression: was hardcoded "parameter".
		let e = Expression::Parameter(ParameterExpression::Positional {
			fragment: frag("$1"),
		});
		assert_eq!(canonical_name(&e).text(), "$1");
	}

	#[test]
	fn canonical_parameter_named() {
		let e = Expression::Parameter(ParameterExpression::Named {
			fragment: frag(":foo"),
		});
		assert_eq!(canonical_name(&e).text(), ":foo");
	}

	#[test]
	fn canonical_variable_dollar() {
		// Regression: was "var_x".
		let e = Expression::Variable(VariableExpression {
			fragment: frag("$x"),
		});
		assert_eq!(canonical_name(&e).text(), "$x");
	}

	#[test]
	fn canonical_prefix_minus_no_space() {
		let e = Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Minus(frag("-")),
			expression: Box::new(col("x")),
			fragment: frag("-"),
		});
		assert_eq!(canonical_name(&e).text(), "-x");
	}

	#[test]
	fn canonical_prefix_not_with_space() {
		let e = Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Not(frag("not")),
			expression: Box::new(col("x")),
			fragment: frag("not"),
		});
		assert_eq!(canonical_name(&e).text(), "not x");
	}

	#[test]
	fn canonical_if_else_chain() {
		let e = Expression::If(IfExpression {
			condition: Box::new(col("c")),
			then_expr: Box::new(col("t")),
			else_ifs: vec![ElseIfExpression {
				condition: Box::new(col("c2")),
				then_expr: Box::new(col("t2")),
				fragment: frag("else if"),
			}],
			else_expr: Some(Box::new(col("e"))),
			fragment: frag("if"),
		});
		assert_eq!(canonical_name(&e).text(), "if c then t else if c2 then t2 else e");
	}

	#[test]
	fn canonical_if_no_else() {
		let e = Expression::If(IfExpression {
			condition: Box::new(col("c")),
			then_expr: Box::new(col("t")),
			else_ifs: vec![],
			else_expr: None,
			fragment: frag("if"),
		});
		assert_eq!(canonical_name(&e).text(), "if c then t");
	}

	#[test]
	fn canonical_alias_transparent() {
		// Aliasing does NOT change canonical_name - the alias is purely
		// for display.
		let inner = add(col("a"), col("b"));
		let aliased = Expression::Alias(AliasExpression {
			alias: IdentExpression(frag("sum")),
			expression: Box::new(inner.clone()),
			fragment: frag("as"),
		});
		assert_eq!(canonical_name(&aliased).text(), canonical_name(&inner).text());
		assert_eq!(canonical_name(&aliased).text(), "a + b");
	}

	#[test]
	fn canonical_access_source_flat_underscore() {
		let e = Expression::AccessSource(AccessShapeExpression {
			column: ColumnIdentifier {
				shape: ColumnShape::Alias(frag("u")),
				name: frag("col"),
			},
		});
		// Flat - underscore separator, no dot.
		assert_eq!(canonical_name(&e).text(), "u_col");
	}

	#[test]
	fn canonical_access_source_qualified_uses_table_name() {
		let e = Expression::AccessSource(AccessShapeExpression {
			column: ColumnIdentifier {
				shape: ColumnShape::Qualified {
					namespace: frag("ns"),
					name: frag("users"),
				},
				name: frag("id"),
			},
		});
		assert_eq!(canonical_name(&e).text(), "users_id");
	}

	#[test]
	fn canonical_field_access_keeps_dot() {
		// FieldAccess (struct field) is syntactically dotted - this is
		// distinct from AccessSource (which is flat in column names).
		let e = Expression::FieldAccess(FieldAccessExpression {
			object: Box::new(col("record")),
			field: frag("name"),
			fragment: frag("."),
		});
		assert_eq!(canonical_name(&e).text(), "record.name");
	}

	#[test]
	fn canonical_sumtype_constructor() {
		let e = Expression::SumTypeConstructor(SumTypeConstructorExpression {
			namespace: frag("ns"),
			sumtype_name: frag("Status"),
			variant_name: frag("Active"),
			columns: vec![(frag("amount"), num("100"))],
			fragment: frag("Status::Active"),
		});
		assert_eq!(canonical_name(&e).text(), "Status::Active(amount: 100)");
	}

	#[test]
	fn canonical_is_variant_with_namespace() {
		let e = Expression::IsVariant(IsVariantExpression {
			expression: Box::new(col("x")),
			namespace: Some(frag("ns")),
			sumtype_name: frag("Status"),
			variant_name: frag("Active"),
			tag: None,
			fragment: frag("is"),
		});
		assert_eq!(canonical_name(&e).text(), "x is ns.Status::Active");
	}

	#[test]
	fn canonical_is_variant_without_namespace() {
		let e = Expression::IsVariant(IsVariantExpression {
			expression: Box::new(col("x")),
			namespace: None,
			sumtype_name: frag("Status"),
			variant_name: frag("Active"),
			tag: None,
			fragment: frag("is"),
		});
		assert_eq!(canonical_name(&e).text(), "x is Status::Active");
	}

	#[test]
	fn display_alias_visible() {
		// Aliasing DOES change display_label.
		let e = Expression::Alias(AliasExpression {
			alias: IdentExpression(frag("sum")),
			expression: Box::new(add(col("a"), col("b"))),
			fragment: frag("as"),
		});
		assert_eq!(display_label(&e).text(), "sum");
	}

	#[test]
	fn display_column_returns_name() {
		assert_eq!(display_label(&col("age")).text(), "age");
	}

	#[test]
	fn display_constant_text_with_quotes() {
		assert_eq!(display_label(&text("hello")).text(), "\"hello\"");
	}

	#[test]
	fn display_constant_number_verbatim() {
		assert_eq!(display_label(&num("42")).text(), "42");
	}

	#[test]
	fn display_compound_falls_back_to_canonical() {
		// Display does NOT merge child fragments. For compound exprs it
		// returns canonical (deterministic) instead.
		let e = add(text("Hello "), text("World"));
		assert_eq!(display_label(&e).text(), "\"Hello \" + \"World\"");
		// Notably NOT "Hello +World" which is what full_fragment_owned() produces.
	}

	#[test]
	fn display_access_source_flat() {
		let e = Expression::AccessSource(AccessShapeExpression {
			column: ColumnIdentifier {
				shape: ColumnShape::Alias(frag("u")),
				name: frag("col"),
			},
		});
		assert_eq!(display_label(&e).text(), "u_col");
	}

	#[test]
	fn display_variable_includes_dollar() {
		let e = Expression::Variable(VariableExpression {
			fragment: frag("$x"),
		});
		assert_eq!(display_label(&e).text(), "$x");
	}

	#[test]
	fn column_collects_name() {
		let result = collect(&col("age"));
		assert_eq!(result, HashSet::from(["age".to_string()]));
	}

	#[test]
	fn access_source_collects_name() {
		let expr = Expression::AccessSource(AccessShapeExpression {
			column: ColumnIdentifier {
				shape: ColumnShape::Qualified {
					namespace: frag("ns"),
					name: frag("tbl"),
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
		let result = collect(&add(col("a"), col("b")));
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
		let expr = add(add(col("a"), col("a")), col("b"));
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
