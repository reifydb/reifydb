// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use reifydb_type::{fragment::Fragment, value::Value};

use crate::expression::{
	AliasExpression, ConstantExpression, Expression, IdentExpression, IfExpression, PrefixExpression,
	PrefixOperator, name::canonical_name,
};

pub fn fold(expr: &mut Expression) {
	fold_children(expr);
	if let Some(folded) = try_fold(expr) {
		*expr = folded;
	}
}

pub fn fold_projection(expr: &mut Expression) {
	let original = canonical_name(expr).text().to_string();
	let was_aliased = matches!(expr, Expression::Alias(_));
	fold(expr);
	if was_aliased {
		return;
	}
	let new_label = canonical_name(expr).text().to_string();
	if new_label == original {
		return;
	}
	let placeholder = Expression::Constant(ConstantExpression::None {
		fragment: Fragment::internal("none"),
	});
	let inner = mem::replace(expr, placeholder);
	*expr = Expression::Alias(AliasExpression {
		alias: IdentExpression(Fragment::internal(original)),
		expression: Box::new(inner),
		fragment: Fragment::internal(""),
	});
}

fn fold_children(expr: &mut Expression) {
	match expr {
		Expression::Add(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Sub(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Mul(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Div(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Rem(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Equal(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::NotEqual(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::GreaterThan(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::GreaterThanEqual(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::LessThan(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::LessThanEqual(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::And(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Or(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Xor(e) => {
			fold(&mut e.left);
			fold(&mut e.right);
		}
		Expression::Prefix(e) => fold(&mut e.expression),
		Expression::Alias(e) => fold(&mut e.expression),
		Expression::Cast(e) => fold(&mut e.expression),
		Expression::Between(e) => {
			fold(&mut e.value);
			fold(&mut e.lower);
			fold(&mut e.upper);
		}
		Expression::In(e) => {
			fold(&mut e.value);
			fold(&mut e.list);
		}
		Expression::Contains(e) => {
			fold(&mut e.value);
			fold(&mut e.list);
		}
		Expression::Tuple(e) => {
			for child in &mut e.expressions {
				fold(child);
			}
		}
		Expression::List(e) => {
			for child in &mut e.expressions {
				fold(child);
			}
		}
		Expression::Map(e) => {
			for child in &mut e.expressions {
				fold(child);
			}
		}
		Expression::Extend(e) => {
			for child in &mut e.expressions {
				fold(child);
			}
		}
		Expression::Call(e) => {
			for arg in &mut e.args {
				fold(arg);
			}
		}
		Expression::If(e) => {
			fold(&mut e.condition);
			fold(&mut e.then_expr);
			for elif in &mut e.else_ifs {
				fold(&mut elif.condition);
				fold(&mut elif.then_expr);
			}
			if let Some(else_expr) = e.else_expr.as_mut() {
				fold(else_expr);
			}
		}
		Expression::FieldAccess(e) => fold(&mut e.object),
		Expression::IsVariant(e) => fold(&mut e.expression),
		Expression::SumTypeConstructor(e) => {
			for (_, value) in &mut e.columns {
				fold(value);
			}
		}
		Expression::Constant(_)
		| Expression::Column(_)
		| Expression::AccessSource(_)
		| Expression::Variable(_)
		| Expression::Parameter(_)
		| Expression::Type(_) => {}
	}
}

fn try_fold(expr: &Expression) -> Option<Expression> {
	match expr {
		Expression::Add(e) => fold_arith(&e.left, &e.right, ArithOp::Add),
		Expression::Sub(e) => fold_arith(&e.left, &e.right, ArithOp::Sub),
		Expression::Mul(e) => fold_arith(&e.left, &e.right, ArithOp::Mul),
		Expression::Div(e) => fold_arith(&e.left, &e.right, ArithOp::Div),
		Expression::Rem(e) => fold_arith(&e.left, &e.right, ArithOp::Rem),
		Expression::Equal(e) => fold_compare(&e.left, &e.right, CmpOp::Eq),
		Expression::NotEqual(e) => fold_compare(&e.left, &e.right, CmpOp::Ne),
		Expression::GreaterThan(e) => fold_compare(&e.left, &e.right, CmpOp::Gt),
		Expression::GreaterThanEqual(e) => fold_compare(&e.left, &e.right, CmpOp::Ge),
		Expression::LessThan(e) => fold_compare(&e.left, &e.right, CmpOp::Lt),
		Expression::LessThanEqual(e) => fold_compare(&e.left, &e.right, CmpOp::Le),
		Expression::And(e) => fold_logic(&e.left, &e.right, LogicOp::And),
		Expression::Or(e) => fold_logic(&e.left, &e.right, LogicOp::Or),
		Expression::Xor(e) => fold_logic(&e.left, &e.right, LogicOp::Xor),
		Expression::Prefix(e) => fold_prefix(e),
		Expression::If(e) => fold_if(e),
		_ => None,
	}
}

#[derive(Copy, Clone)]
enum ArithOp {
	Add,
	Sub,
	Mul,
	Div,
	Rem,
}

#[derive(Copy, Clone)]
enum CmpOp {
	Eq,
	Ne,
	Gt,
	Ge,
	Lt,
	Le,
}

#[derive(Copy, Clone)]
enum LogicOp {
	And,
	Or,
	Xor,
}

fn fold_arith(left: &Expression, right: &Expression, op: ArithOp) -> Option<Expression> {
	let lv = as_constant(left)?.to_value();
	let rv = as_constant(right)?.to_value();
	let li = value_as_i128(&lv)?;
	let ri = value_as_i128(&rv)?;
	let result = match op {
		ArithOp::Add => li.checked_add(ri)?,
		ArithOp::Sub => li.checked_sub(ri)?,
		ArithOp::Mul => li.checked_mul(ri)?,
		ArithOp::Div => {
			if ri == 0 {
				return None;
			}
			li.checked_div(ri)?
		}
		ArithOp::Rem => {
			if ri == 0 {
				return None;
			}
			li.checked_rem(ri)?
		}
	};
	Some(int_constant(result))
}

fn fold_compare(left: &Expression, right: &Expression, op: CmpOp) -> Option<Expression> {
	let lc = as_constant(left)?;
	let rc = as_constant(right)?;
	if matches!(lc, ConstantExpression::Temporal { .. }) || matches!(rc, ConstantExpression::Temporal { .. }) {
		return None;
	}
	let lv = lc.to_value();
	let rv = rc.to_value();
	let ord = match (&lv, &rv) {
		(Value::Boolean(a), Value::Boolean(b)) => (*a as u8).cmp(&(*b as u8)),
		(Value::Utf8(a), Value::Utf8(b)) => a.cmp(b),
		_ => {
			let li = value_as_i128(&lv)?;
			let ri = value_as_i128(&rv)?;
			li.cmp(&ri)
		}
	};
	let result = match op {
		CmpOp::Eq => ord.is_eq(),
		CmpOp::Ne => ord.is_ne(),
		CmpOp::Gt => ord.is_gt(),
		CmpOp::Ge => ord.is_ge(),
		CmpOp::Lt => ord.is_lt(),
		CmpOp::Le => ord.is_le(),
	};
	Some(bool_constant(result))
}

fn fold_logic(left: &Expression, right: &Expression, op: LogicOp) -> Option<Expression> {
	let lb = as_bool_constant(left);
	let rb = as_bool_constant(right);
	match (lb, rb, op) {
		(Some(a), Some(b), LogicOp::And) => Some(bool_constant(a && b)),
		(Some(a), Some(b), LogicOp::Or) => Some(bool_constant(a || b)),
		(Some(a), Some(b), LogicOp::Xor) => Some(bool_constant(a != b)),

		(Some(false), _, LogicOp::And) => Some(bool_constant(false)),
		(_, Some(false), LogicOp::And) => Some(bool_constant(false)),
		(Some(true), _, LogicOp::And) => Some(right.clone()),
		(_, Some(true), LogicOp::And) => Some(left.clone()),

		(Some(true), _, LogicOp::Or) => Some(bool_constant(true)),
		(_, Some(true), LogicOp::Or) => Some(bool_constant(true)),
		(Some(false), _, LogicOp::Or) => Some(right.clone()),
		(_, Some(false), LogicOp::Or) => Some(left.clone()),
		_ => None,
	}
}

fn fold_prefix(e: &PrefixExpression) -> Option<Expression> {
	let v = as_constant(&e.expression)?.to_value();
	match (&e.operator, v) {
		(PrefixOperator::Not(_), Value::Boolean(b)) => Some(bool_constant(!b)),
		(PrefixOperator::Minus(_), val) => {
			let i = value_as_i128(&val)?;
			let neg = i.checked_neg()?;
			Some(int_constant(neg))
		}
		(PrefixOperator::Plus(_), val) => {
			let i = value_as_i128(&val)?;
			Some(int_constant(i))
		}
		_ => None,
	}
}

fn fold_if(e: &IfExpression) -> Option<Expression> {
	match as_bool_constant(&e.condition) {
		Some(true) => return Some((*e.then_expr).clone()),
		None => return None,
		Some(false) => {}
	}
	for elif in &e.else_ifs {
		match as_bool_constant(&elif.condition) {
			Some(true) => return Some((*elif.then_expr).clone()),
			Some(false) => continue,
			None => return None,
		}
	}
	Some(match &e.else_expr {
		Some(else_expr) => (**else_expr).clone(),
		None => Expression::Constant(ConstantExpression::None {
			fragment: Fragment::internal("none"),
		}),
	})
}

fn as_constant(expr: &Expression) -> Option<&ConstantExpression> {
	match expr {
		Expression::Constant(c) => Some(c),
		Expression::Alias(a) => as_constant(&a.expression),
		_ => None,
	}
}

fn as_bool_constant(expr: &Expression) -> Option<bool> {
	match as_constant(expr)?.to_value() {
		Value::Boolean(b) => Some(b),
		_ => None,
	}
}

fn value_as_i128(v: &Value) -> Option<i128> {
	match v {
		Value::Int1(x) => Some(*x as i128),
		Value::Int2(x) => Some(*x as i128),
		Value::Int4(x) => Some(*x as i128),
		Value::Int8(x) => Some(*x as i128),
		Value::Int16(x) => Some(*x),
		Value::Uint1(x) => Some(*x as i128),
		Value::Uint2(x) => Some(*x as i128),
		Value::Uint4(x) => Some(*x as i128),
		Value::Uint8(x) => Some(*x as i128),
		Value::Uint16(x) if *x <= i128::MAX as u128 => Some(*x as i128),
		_ => None,
	}
}

fn int_constant(v: i128) -> Expression {
	Expression::Constant(ConstantExpression::Number {
		fragment: Fragment::internal(v.to_string()),
	})
}

fn bool_constant(b: bool) -> Expression {
	Expression::Constant(ConstantExpression::Bool {
		fragment: Fragment::internal(if b {
			"true"
		} else {
			"false"
		}),
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnShape};
	use reifydb_type::fragment::Fragment;

	use super::*;
	use crate::expression::{
		AddExpression, AndExpression, CallExpression, ColumnExpression, ConstantExpression, DivExpression,
		ElseIfExpression, EqExpression, GreaterThanExpression, IdentExpression, IfExpression, MulExpression,
		OrExpression, PrefixExpression, PrefixOperator,
	};

	fn frag(s: &str) -> Fragment {
		Fragment::internal(s)
	}

	fn num(n: &str) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: frag(n),
		})
	}

	fn boolean(s: &str) -> Expression {
		Expression::Constant(ConstantExpression::Bool {
			fragment: frag(s),
		})
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

	fn add(l: Expression, r: Expression) -> Expression {
		Expression::Add(AddExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("+"),
		})
	}

	fn mul(l: Expression, r: Expression) -> Expression {
		Expression::Mul(MulExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("*"),
		})
	}

	fn gt(l: Expression, r: Expression) -> Expression {
		Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag(">"),
		})
	}

	fn eq(l: Expression, r: Expression) -> Expression {
		Expression::Equal(EqExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("=="),
		})
	}

	fn and(l: Expression, r: Expression) -> Expression {
		Expression::And(AndExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("and"),
		})
	}

	fn or(l: Expression, r: Expression) -> Expression {
		Expression::Or(OrExpression {
			left: Box::new(l),
			right: Box::new(r),
			fragment: frag("or"),
		})
	}

	fn not(e: Expression) -> Expression {
		Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Not(frag("not")),
			expression: Box::new(e),
			fragment: frag("not"),
		})
	}

	fn neg(e: Expression) -> Expression {
		Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Minus(frag("-")),
			expression: Box::new(e),
			fragment: frag("-"),
		})
	}

	fn assert_constant_number(expr: &Expression, expected: &str) {
		match expr {
			Expression::Constant(ConstantExpression::Number {
				fragment,
			}) => assert_eq!(fragment.text(), expected),
			other => panic!("expected Constant(Number {:?}), got {:?}", expected, other),
		}
	}

	fn assert_constant_bool(expr: &Expression, expected: bool) {
		match expr {
			Expression::Constant(ConstantExpression::Bool {
				fragment,
			}) => assert_eq!(
				fragment.text(),
				if expected {
					"true"
				} else {
					"false"
				}
			),
			other => panic!("expected Constant(Bool {}), got {:?}", expected, other),
		}
	}

	#[test]
	fn folds_int_addition() {
		let mut e = add(num("1"), num("2"));
		fold(&mut e);
		assert_constant_number(&e, "3");
	}

	#[test]
	fn folds_nested_int_arith() {
		let mut e = add(num("1"), mul(num("2"), num("3")));
		fold(&mut e);
		assert_constant_number(&e, "7");
	}

	#[test]
	fn partial_fold_with_column() {
		let mut e = add(add(num("1"), num("2")), col("x"));
		fold(&mut e);
		match &e {
			Expression::Add(a) => {
				assert_constant_number(&a.left, "3");
				assert!(matches!(&*a.right, Expression::Column(_)));
			}
			other => panic!("expected Add, got {:?}", other),
		}
	}

	#[test]
	fn folds_comparison_int() {
		let mut e = gt(num("5"), num("3"));
		fold(&mut e);
		assert_constant_bool(&e, true);
	}

	#[test]
	fn folds_comparison_eq_bool() {
		let mut e = eq(boolean("true"), boolean("true"));
		fold(&mut e);
		assert_constant_bool(&e, true);
	}

	#[test]
	fn folds_logic_and_both_known() {
		let mut e = and(boolean("true"), boolean("false"));
		fold(&mut e);
		assert_constant_bool(&e, false);
	}

	#[test]
	fn algebraic_true_and_col_to_col() {
		let mut e = and(boolean("true"), col("x"));
		fold(&mut e);
		assert!(matches!(e, Expression::Column(_)));
	}

	#[test]
	fn algebraic_false_and_col_to_false() {
		let mut e = and(boolean("false"), col("x"));
		fold(&mut e);
		assert_constant_bool(&e, false);
	}

	#[test]
	fn algebraic_false_or_col_to_col() {
		let mut e = or(boolean("false"), col("x"));
		fold(&mut e);
		assert!(matches!(e, Expression::Column(_)));
	}

	#[test]
	fn algebraic_true_or_col_to_true() {
		let mut e = or(boolean("true"), col("x"));
		fold(&mut e);
		assert_constant_bool(&e, true);
	}

	#[test]
	fn folds_not_true_to_false() {
		let mut e = not(boolean("true"));
		fold(&mut e);
		assert_constant_bool(&e, false);
	}

	#[test]
	fn folds_negation_of_int() {
		let mut e = neg(num("5"));
		fold(&mut e);
		assert_constant_number(&e, "-5");
	}

	#[test]
	fn dead_branch_if_true_takes_then() {
		let mut e = Expression::If(IfExpression {
			condition: Box::new(boolean("true")),
			then_expr: Box::new(col("x")),
			else_ifs: vec![],
			else_expr: Some(Box::new(col("y"))),
			fragment: frag("if"),
		});
		fold(&mut e);
		match &e {
			Expression::Column(c) => assert_eq!(c.0.name.text(), "x"),
			other => panic!("expected col(x), got {:?}", other),
		}
	}

	#[test]
	fn dead_branch_if_false_else_if_true_takes_elif_then() {
		let mut e = Expression::If(IfExpression {
			condition: Box::new(boolean("false")),
			then_expr: Box::new(col("x")),
			else_ifs: vec![ElseIfExpression {
				condition: Box::new(boolean("true")),
				then_expr: Box::new(col("y")),
				fragment: frag("else if"),
			}],
			else_expr: Some(Box::new(col("z"))),
			fragment: frag("if"),
		});
		fold(&mut e);
		match &e {
			Expression::Column(c) => assert_eq!(c.0.name.text(), "y"),
			other => panic!("expected col(y), got {:?}", other),
		}
	}

	#[test]
	fn dead_branch_if_false_no_else_yields_none() {
		let mut e = Expression::If(IfExpression {
			condition: Box::new(boolean("false")),
			then_expr: Box::new(col("x")),
			else_ifs: vec![],
			else_expr: None,
			fragment: frag("if"),
		});
		fold(&mut e);
		assert!(matches!(e, Expression::Constant(ConstantExpression::None { .. })));
	}

	#[test]
	fn if_with_non_constant_condition_unchanged() {
		let mut e = Expression::If(IfExpression {
			condition: Box::new(col("c")),
			then_expr: Box::new(col("x")),
			else_ifs: vec![],
			else_expr: Some(Box::new(col("y"))),
			fragment: frag("if"),
		});
		fold(&mut e);
		assert!(matches!(e, Expression::If(_)));
	}

	#[test]
	fn does_not_fold_when_column_present() {
		let mut e = add(col("x"), num("1"));
		fold(&mut e);
		assert!(matches!(e, Expression::Add(_)));
	}

	#[test]
	fn does_not_fold_through_call() {
		let mut e = Expression::Call(CallExpression {
			func: IdentExpression(frag("len")),
			args: vec![num("1"), num("2")],
			fragment: frag("len()"),
		});
		fold(&mut e);
		// Children fold, but Call itself is not foldable.
		assert!(matches!(e, Expression::Call(_)));
	}

	#[test]
	fn does_not_fold_div_by_zero() {
		let mut e = Expression::Div(DivExpression {
			left: Box::new(num("10")),
			right: Box::new(num("0")),
			fragment: frag("/"),
		});
		fold(&mut e);
		assert!(matches!(e, Expression::Div(_)));
	}
}
