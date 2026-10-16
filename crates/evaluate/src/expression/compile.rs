// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::discriminant;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::{
	expression::VariableExpression,
	value::{
		batch::{is_scalar, scalar_value},
		column::{builder::ColumnBuilder, cast::error::CastError, factory},
	},
};
use reifydb_value::{
	error::{BinaryOp, Error, IntoDiagnostic, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::{Constraint, TypeConstraint, precision::Precision, scale::Scale},
		system_columns::user_columns,
		value_type::ValueType,
	},
};

use super::option::binary_op_unwrap_option;
use crate::{
	Result,
	expression::{
		compare::{Equal, compare_columns, length_mismatch},
		context::EvalContext,
	},
	stack::Variable,
};

pub(crate) fn variable_column(
	ctx: &EvalContext,
	expr: &VariableExpression,
	row_count: usize,
) -> Result<(FieldRef, ArrayRef)> {
	let variable_name = expr.name();

	if variable_name == "env" {
		return Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableIsDataframe {
				name: variable_name.to_string(),
			},
			message: format!(
				"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
				variable_name
			),
		}
		.into());
	}

	match ctx.symbols.get(variable_name) {
		Some(Variable::Columns {
			batch,
		}) if is_scalar(batch) => {
			let value = match scalar_value(batch)? {
				Value::Any(inner)
					if matches!(
						inner.as_ref(),
						Value::List(items)
							if items.iter().all(|v| matches!(v, Value::Record(_)))
					) =>
				{
					*inner
				}
				other => other,
			};
			let mut data = ColumnBuilder::with_capacity(value.get_type(), row_count);
			for _ in 0..row_count {
				data.push_value(value.clone());
			}
			Ok(data.finish(variable_name))
		}
		Some(Variable::Columns {
			..
		})
		| Some(Variable::ForIterator {
			..
		})
		| Some(Variable::Closure(_)) => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableIsDataframe {
				name: variable_name.to_string(),
			},
			message: format!(
				"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
				variable_name
			),
		}
		.into()),
		None => {
			if let Some(value) = ctx.params.get_named(variable_name) {
				let mut data = ColumnBuilder::with_capacity(value.get_type(), row_count);
				for _ in 0..row_count {
					data.push_value(value.clone());
				}
				return Ok(data.finish(variable_name));
			}
			Err(TypeError::Runtime {
				kind: RuntimeErrorKind::VariableNotFound {
					fragment: expr.fragment.clone(),
				},
				message: format!("Variable '{}' is not defined", variable_name),
			}
			.into())
		}
	}
}

pub(crate) fn variable_field_column(
	ctx: &EvalContext,
	variable: &VariableExpression,
	field_name: &str,
	row_count: usize,
) -> Result<(FieldRef, ArrayRef)> {
	let variable_name = variable.name();
	match ctx.symbols.get(variable_name) {
		Some(Variable::Columns {
			batch,
		}) if !is_scalar(batch) => {
			let found = user_columns(batch).find(|(field, _)| field.name() == field_name);
			match found {
				Some((field, array)) => {
					let value = ColumnView::try_from((array, field.as_ref()))?.get_value(0);
					let mut data = ColumnBuilder::with_capacity(value.get_type(), row_count);
					for _ in 0..row_count {
						data.push_value(value.clone());
					}
					Ok(data.finish(field_name))
				}
				None => {
					let available: Vec<String> = user_columns(batch)
						.map(|(field, _)| field.name().to_string())
						.collect();
					Err(TypeError::Runtime {
						kind: RuntimeErrorKind::FieldNotFound {
							variable: variable_name.to_string(),
							field: field_name.to_string(),
							available,
						},
						message: format!(
							"Field '{}' not found on variable '{}'",
							field_name, variable_name
						),
					}
					.into())
				}
			}
		}
		Some(Variable::Columns {
			..
		})
		| Some(Variable::Closure(_)) => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::FieldNotFound {
				variable: variable_name.to_string(),
				field: field_name.to_string(),
				available: vec![],
			},
			message: format!("Field '{}' not found on variable '{}'", field_name, variable_name),
		}
		.into()),
		Some(Variable::ForIterator {
			..
		}) => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableIsDataframe {
				name: variable_name.to_string(),
			},
			message: format!(
				"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
				variable_name
			),
		}
		.into()),
		None => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableNotFound {
				fragment: variable.fragment.clone(),
			},
			message: format!("Variable '{}' is not defined", variable_name),
		}
		.into()),
	}
}

pub(crate) fn type_column(row_count: usize, ty: &ValueType, fragment: &Fragment) -> (FieldRef, ArrayRef) {
	let values: Vec<Value> = (0..row_count).map(|_| Value::Type(ty.clone())).collect();
	factory::any(fragment.text(), values)
}

pub(crate) fn combine_bool_columns(
	left: (FieldRef, ArrayRef),
	right: (FieldRef, ArrayRef),
	fragment: Fragment,
	combine_fn: fn(bool, bool) -> bool,
) -> Result<(FieldRef, ArrayRef)> {
	if left.1.len() != right.1.len() {
		return Err(length_mismatch(left.1.len(), right.1.len(), &fragment));
	}

	binary_op_unwrap_option(&left, &right, fragment.clone(), |left, right| {
		let (left, right) = (ColumnView::try_from(left)?, ColumnView::try_from(right)?);
		match (&left.data, &right.data) {
			(ViewData::Bool(l), ViewData::Bool(r)) => {
				let len = l.len();
				let mut data = Vec::with_capacity(len);
				let mut bitvec = Vec::with_capacity(len);

				for i in 0..len {
					data.push(combine_fn(l.value(i), r.value(i)));
					bitvec.push(true);
				}

				Ok(factory::bool_with_bitvec(fragment.text(), data, bitvec))
			}
			_ => {
				unreachable!("combine_bool_columns should only be called with boolean columns")
			}
		}
	})
}

fn list_items_contain(items: &[Value], element: &Value, fragment: &Fragment) -> bool {
	if items.iter().any(|item| item == element) {
		return true;
	}
	if items.is_empty() {
		return false;
	}

	if let Some(items_col) = build_homogeneous_buffer(items, fragment.text()) {
		let elems_col = factory::from_many(fragment.text(), element.clone(), items.len());
		return compare_columns::<Equal>(&items_col, &elems_col, fragment.clone(), |f, l, r| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Equal,
				left: l,
				right: r,
				fragment: f,
			}
			.into_diagnostic()
		})
		.and_then(|c| bool_column_has_true(&c))
		.unwrap_or(false);
	}

	list_items_contain_per_item(items, element, fragment)
}

fn list_items_contain_per_item(items: &[Value], element: &Value, fragment: &Fragment) -> bool {
	items.iter().any(|item| {
		let item_col = factory::from_one(fragment.text(), item.clone());
		let elem_col = factory::from_one(fragment.text(), element.clone());
		compare_columns::<Equal>(&item_col, &elem_col, fragment.clone(), |f, l, r| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Equal,
				left: l,
				right: r,
				fragment: f,
			}
			.into_diagnostic()
		})
		.and_then(|c| {
			let view = ColumnView::try_from(&c)?;
			Ok(match &view.data {
				ViewData::Bool(b) if view.logical_nulls().is_none() => b.value(0),
				_ => false,
			})
		})
		.unwrap_or(false)
	})
}

fn bool_column_has_true(col: &(FieldRef, ArrayRef)) -> Result<bool> {
	let view = ColumnView::try_from(col)?;
	Ok(match &view.data {
		ViewData::Bool(b) => match view.logical_nulls() {
			Some(nulls) => (nulls.inner() & b.values()).has_true(),
			None => b.values().has_true(),
		},
		_ => false,
	})
}

fn build_homogeneous_buffer(items: &[Value], name: &str) -> Option<(FieldRef, ArrayRef)> {
	let first = items.first()?;
	let first_disc = discriminant(first);
	if !items.iter().all(|v| discriminant(v) == first_disc) {
		return None;
	}

	macro_rules! collect {
		($variant:ident, $constructor:ident $([$($arg:expr),*])?, |$x:ident| $convert:expr) => {{
			let data: Vec<_> = items
				.iter()
				.map(|v| match v {
					Value::$variant($x) => $convert,
					_ => unreachable!("homogeneous check guarantees variant"),
				})
				.collect();
			Some(factory::$constructor(name, $($($arg,)*)? data))
		}};
	}

	match first {
		Value::Boolean(_) => collect!(Boolean, bool, |x| *x),
		Value::Float4(_) => collect!(Float4, float4, |x| x.value()),
		Value::Float8(_) => collect!(Float8, float8, |x| x.value()),
		Value::Int1(_) => collect!(Int1, int1, |x| *x),
		Value::Int2(_) => collect!(Int2, int2, |x| *x),
		Value::Int4(_) => collect!(Int4, int4, |x| *x),
		Value::Int8(_) => collect!(Int8, int8, |x| *x),
		Value::Int16(_) => collect!(Int16, int16, |x| *x),
		Value::Uint1(_) => collect!(Uint1, uint1, |x| *x),
		Value::Uint2(_) => collect!(Uint2, uint2, |x| *x),
		Value::Uint4(_) => collect!(Uint4, uint4, |x| *x),
		Value::Uint8(_) => collect!(Uint8, uint8, |x| *x),
		Value::Uint16(_) => collect!(Uint16, uint16, |x| *x),
		Value::Utf8(_) => collect!(Utf8, utf8, |x| x.clone()),
		Value::Date(_) => collect!(Date, date, |x| *x),
		Value::DateTime(_) => collect!(DateTime, datetime, |x| *x),
		Value::Time(_) => collect!(Time, time, |x| *x),
		Value::Duration(_) => collect!(Duration, duration, |x| *x),
		Value::Uuid4(_) => collect!(Uuid4, uuid4, |x| *x),
		Value::Uuid7(_) => collect!(Uuid7, uuid7, |x| *x),
		Value::IdentityId(_) => collect!(IdentityId, identity_id, |x| *x),
		Value::Blob(_) => collect!(Blob, blob, |x| x.clone()),
		Value::Decimal(_) => {
			let scale = items
				.iter()
				.filter_map(|v| match v {
					Value::Decimal(d) => Some(d.scale()),
					_ => None,
				})
				.max()?;
			if !items.iter().all(|v| matches!(v, Value::Decimal(d) if d.rescale(scale).is_some())) {
				return None;
			}
			collect!(Decimal, decimal[Precision::MAX, Scale::new(scale)], |x| x.clone())
		}
		Value::DictionaryId(_) => collect!(DictionaryId, dictionary_id, |x| *x),

		_ => None,
	}
}

pub(crate) fn list_contains_element(
	list_col: &(FieldRef, ArrayRef),
	element_col: &(FieldRef, ArrayRef),
	fragment: &Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	let (list_view, element_view) = (ColumnView::try_from(list_col)?, ColumnView::try_from(element_col)?);
	let len = list_col.1.len();
	let mut data = Vec::with_capacity(len);

	for i in 0..len {
		let list_value = list_view.get_value(i);
		let element_value = element_view.get_value(i);

		let contained = match &list_value {
			Value::List(items) => list_items_contain(items, &element_value, fragment),
			Value::Tuple(items) => list_items_contain(items, &element_value, fragment),
			Value::Any(boxed) => match boxed.as_ref() {
				Value::List(items) => list_items_contain(items, &element_value, fragment),
				Value::Tuple(items) => list_items_contain(items, &element_value, fragment),
				_ => false,
			},
			_ => false,
		};
		data.push(contained);
	}

	Ok(factory::bool(fragment.text(), data))
}

pub(crate) fn is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::None {
			..
		} => false,
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		Value::Utf8(s) => !s.is_empty(),
		_ => true,
	}
}

pub(crate) fn enforce_cast_constraint(
	column: &(FieldRef, ArrayRef),
	target: &ValueType,
	constraint: &Option<Constraint>,
	fragment: &Fragment,
) -> Result<()> {
	let Some(constraint) = constraint else {
		return Ok(());
	};
	let checked = TypeConstraint::with_constraint(target.clone(), constraint.clone());
	let view = ColumnView::try_from(column)?;
	for row in (0..view.len()).filter(|&row| !view.none_at(row)) {
		if let Err(mut error) = checked.coerce(&mut view.get_value(row)) {
			error.0.fragment = fragment.clone();
			return Err(error);
		}
	}
	Ok(())
}

pub(crate) fn wrap_cast_error(err: Error, fragment: Fragment, target: &ValueType) -> Error {
	if err.0.code.starts_with("CAST_") {
		return err;
	}
	let cause = err.diagnostic();
	let wrapped = if target.is_bool() {
		CastError::InvalidBoolean {
			fragment,
			cause,
		}
	} else if target.is_temporal() {
		CastError::InvalidTemporal {
			fragment,
			target: target.clone(),
			cause,
		}
	} else if target.is_uuid() || *target == ValueType::IdentityId {
		CastError::InvalidUuid {
			fragment,
			target: target.clone(),
			cause,
		}
	} else {
		CastError::InvalidNumber {
			fragment,
			target: target.clone(),
			cause,
		}
	};
	Error::from(wrapped)
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use arrow_array::{ArrayRef, RecordBatch};
	use arrow_schema::FieldRef;
	use reifydb_core::{
		expression::{
			CastExpression, ColumnExpression, ConstantExpression, ElseIfExpression, Expression,
			IfExpression, MapExpression, TypeExpression,
		},
		interface::identifier::ColumnIdentifier,
		value::{batch::batch, column::factory},
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{
			Value,
			column_view::ColumnView,
			constraint::{precision::Precision, scale::Scale},
			decimal::Decimal,
			value_type::ValueType,
		},
	};

	use super::combine_bool_columns;
	use crate::expression::{context::EvalContext, eval::evaluate};

	fn column(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier::with_alias(
			Fragment::internal("t"),
			Fragment::internal(name),
		)))
	}

	fn uncastable() -> Expression {
		Expression::Cast(CastExpression {
			fragment: Fragment::testing_empty(),
			expression: Box::new(Expression::Constant(ConstantExpression::Text {
				fragment: Fragment::internal("not-a-number"),
			})),
			to: TypeExpression {
				fragment: Fragment::testing_empty(),
				ty: ValueType::Int4,
				constraint: None,
			},
		})
	}

	fn conditional(
		condition: Expression,
		then_expr: Expression,
		else_ifs: Vec<(Expression, Expression)>,
		else_expr: Option<Expression>,
	) -> Expression {
		Expression::If(IfExpression {
			condition: Box::new(condition),
			then_expr: Box::new(then_expr),
			else_ifs: else_ifs
				.into_iter()
				.map(|(condition, then_expr)| ElseIfExpression {
					condition: Box::new(condition),
					then_expr: Box::new(then_expr),
					fragment: Fragment::testing_empty(),
				})
				.collect(),
			else_expr: else_expr.map(Box::new),
			fragment: Fragment::testing_empty(),
		})
	}

	fn bools(name: &str, data: [bool; 4]) -> (FieldRef, ArrayRef) {
		factory::bool(name, data)
	}

	fn ints(name: &str, data: [i32; 4]) -> (FieldRef, ArrayRef) {
		factory::int4(name, data)
	}

	#[test]
	fn every_row_reads_its_own_index_from_the_branch_it_selected() {
		// A branch is evaluated as a whole column, so row i must take branch[i]. Assembling the
		// result in append order instead of by row index shifts every value after the first switch.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![
				bools("flag", [true, false, false, true]),
				ints("hi", [100, 200, 300, 400]),
				ints("lo", [1, 2, 3, 4]),
			])
			.unwrap(),
			4,
		);

		let result =
			evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], Some(column("lo")))).unwrap();

		assert_eq!(result.1.as_ref(), factory::int4("", [100, 2, 3, 400]).1.as_ref());
	}

	#[test]
	fn an_else_if_chain_gives_each_row_its_first_matching_branch() {
		// Later conditions must never override an earlier match: row 1 satisfies both `second` and
		// nothing else, row 2 satisfies `second` alone, and row 3 falls through to the else.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![
				bools("first", [true, false, false, false]),
				bools("second", [true, true, true, false]),
				ints("a", [10, 20, 30, 40]),
				ints("b", [1, 2, 3, 4]),
				ints("c", [-1, -2, -3, -4]),
			])
			.unwrap(),
			4,
		);

		let result = evaluate(
			&ctx,
			&conditional(
				column("first"),
				column("a"),
				vec![(column("second"), column("b"))],
				Some(column("c")),
			),
		)
		.unwrap();

		assert_eq!(result.1.as_ref(), factory::int4("", [10, 2, 3, -4]).1.as_ref());
	}

	#[test]
	fn a_row_that_matches_no_branch_becomes_none() {
		// Without an else branch the unmatched rows must still occupy their slot, otherwise the
		// result column is shorter than the input and every downstream row pairs with the wrong key.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![bools("flag", [true, false, false, true]), ints("hi", [7, 8, 9, 10])]).unwrap(),
			4,
		);

		let result = evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], None)).unwrap();

		let view = ColumnView::try_from(&result).unwrap();
		assert_eq!(result.1.len(), 4);
		assert_eq!(view.get_value(0), Value::Int4(7));
		assert!(matches!(view.get_value(1), Value::None { .. }));
		assert!(matches!(view.get_value(2), Value::None { .. }));
		assert_eq!(view.get_value(3), Value::Int4(10));
	}

	#[test]
	fn a_branch_that_no_row_selects_is_never_evaluated() {
		// Hoisting a branch out of the row loop must not make it eager: a guard exists precisely to
		// keep a failing expression away from the rows that cannot satisfy it.
		let base = EvalContext::testing();
		let all_true = base.with_eval(
			batch(vec![bools("flag", [true, true, true, true]), ints("hi", [1, 2, 3, 4])]).unwrap(),
			4,
		);

		let skipped =
			evaluate(&all_true, &conditional(column("flag"), column("hi"), vec![], Some(uncastable())));

		assert!(skipped.is_ok(), "an else branch no row selects must not be evaluated");

		let one_false = base.with_eval(
			batch(vec![bools("flag", [true, true, false, true]), ints("hi", [1, 2, 3, 4])]).unwrap(),
			4,
		);

		let taken =
			evaluate(&one_false, &conditional(column("flag"), column("hi"), vec![], Some(uncastable())));

		assert!(
			taken.is_err(),
			"the branch must really fail when a row selects it, or the case above is vacuous"
		);
	}

	fn multi(names: [&str; 2]) -> Expression {
		Expression::Map(MapExpression {
			expressions: names.iter().map(|name| column(name)).collect(),
			fragment: Fragment::testing_empty(),
		})
	}

	fn none_literal() -> Expression {
		Expression::Constant(ConstantExpression::None {
			fragment: Fragment::testing_empty(),
		})
	}

	fn four_row_ctx(extra: Vec<(FieldRef, ArrayRef)>) -> RecordBatch {
		let mut cols = vec![bools("flag", [true, false, false, true])];
		cols.extend(extra);
		batch(cols).unwrap()
	}

	#[test]
	fn branches_of_different_types_are_rejected_instead_of_aborting() {
		// The column buffer matches the value variant exactly and panics on anything else, so an
		// unvalidated mismatch takes the process down rather than failing the query.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			four_row_ctx(vec![ints("small", [1, 2, 3, 4]), factory::int8("wide", [5i64, 6, 7, 8])]),
			4,
		);

		let err = evaluate(&ctx, &conditional(column("flag"), column("small"), vec![], Some(column("wide"))))
			.expect_err("int4 and int8 branches must not be accepted");

		assert_eq!(err.0.code, "RUNTIME_012");
	}

	#[test]
	fn an_optional_branch_still_pairs_with_its_bare_type() {
		// Option is a wrapper over the same base type, so these branches agree; rejecting them
		// would break every conditional whose branches differ only in nullability.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			four_row_ctx(vec![
				ints("bare", [1, 2, 3, 4]),
				factory::int4_with_bitvec("opt", [9, 8, 7, 6], vec![true, false, true, true]),
			]),
			4,
		);

		let result = evaluate(&ctx, &conditional(column("flag"), column("bare"), vec![], Some(column("opt"))))
			.expect("a bare and an optional branch of one base type must agree");

		assert_eq!(result.1.len(), 4);
	}

	#[test]
	fn a_none_branch_widens_to_the_other_branch_type() {
		// A none literal carries Option(Any); treating Any as a concrete type would reject the
		// guarded-value shape that every conditional projection relies on.
		let base = EvalContext::testing();
		let ctx = base.with_eval(four_row_ctx(vec![ints("hi", [7, 8, 9, 10])]), 4);

		let result = evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], Some(none_literal())))
			.expect("a none branch must widen to the other branch type");

		let view = ColumnView::try_from(&result).unwrap();
		assert_eq!(view.get_value(0), Value::Int4(7));
		assert!(matches!(view.get_value(1), Value::None { .. }));
	}

	#[test]
	fn a_none_first_row_does_not_fix_the_result_type() {
		// Typing the result from the first row's none branch made it Any, so the first int after it aborted the
		// process.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![bools("flag", [false, true, false, true]), ints("hi", [7, 8, 9, 10])]).unwrap(),
			4,
		);

		let result = evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], Some(none_literal())))
			.expect("a none first row must not fix the result type");

		let view = ColumnView::try_from(&result).unwrap();
		assert!(matches!(view.get_value(0), Value::None { .. }));
		assert_eq!(view.get_value(1), Value::Int4(8));
		assert!(matches!(view.get_value(2), Value::None { .. }));
		assert_eq!(view.get_value(3), Value::Int4(10));
	}

	#[test]
	fn branches_of_different_widths_are_rejected() {
		// A narrower branch used to leave its unfilled columns short, silently misaligning every
		// value after the first switch; a wider one had its surplus columns dropped.
		let base = EvalContext::testing();
		let ctx = base.with_eval(four_row_ctx(vec![ints("a", [1, 2, 3, 4]), ints("b", [10, 20, 30, 40])]), 4);

		let wide_then =
			evaluate(&ctx, &conditional(column("flag"), multi(["a", "b"]), vec![], Some(column("a"))))
				.expect_err("a two column branch must not pair with a one column branch");
		assert_eq!(wide_then.0.code, "RUNTIME_012");

		let wide_else =
			evaluate(&ctx, &conditional(column("flag"), column("a"), vec![], Some(multi(["a", "b"]))))
				.expect_err("a one column branch must not pair with a two column branch");
		assert_eq!(wide_else.0.code, "RUNTIME_012");
	}

	#[test]
	fn a_mismatched_branch_no_row_selects_is_rejected() {
		// A branch type must not depend on which rows a batch holds, so an unselected mismatch is still an
		// error.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![
				bools("flag", [true, true, true, true]),
				ints("small", [1, 2, 3, 4]),
				factory::int8("wide", [5i64, 6, 7, 8]),
			])
			.unwrap(),
			4,
		);

		let err = evaluate(&ctx, &conditional(column("flag"), column("small"), vec![], Some(column("wide"))))
			.expect_err("an unselected branch of another type must be rejected");

		assert_eq!(err.code, "RUNTIME_012", "expected a conditional branch mismatch, got {err:?}");
	}

	#[test]
	fn decimal_and_float8_branches_unify_to_float8_in_either_order() {
		// A decimal literal next to a float branch must not be a mismatch, and its rows must be cast, never
		// pushed raw.
		let base = EvalContext::testing();
		let decimals = ["10.5", "20.5", "30.5", "40.5"].map(|text| Decimal::from_str(text).unwrap());
		let ctx = base.with_eval(
			four_row_ctx(vec![
				factory::float8("f", [1.5, 2.5, 3.5, 4.5]),
				factory::decimal("d", Precision::new(3), Scale::new(1), decimals),
			]),
			4,
		);

		let float_then = evaluate(&ctx, &conditional(column("flag"), column("f"), vec![], Some(column("d"))))
			.expect("a float then branch must unify with a decimal else branch");
		let view = ColumnView::try_from(&float_then).unwrap();
		assert_eq!(*view.get_type().inner_type(), ValueType::Float8);
		assert_eq!(view.get_value(0), Value::float8(1.5));
		assert_eq!(view.get_value(1), Value::float8(20.5));
		assert_eq!(view.get_value(2), Value::float8(30.5));
		assert_eq!(view.get_value(3), Value::float8(4.5));

		let decimal_then = evaluate(&ctx, &conditional(column("flag"), column("d"), vec![], Some(column("f"))))
			.expect("a decimal then branch must unify with a float else branch");
		let view = ColumnView::try_from(&decimal_then).unwrap();
		assert_eq!(*view.get_type().inner_type(), ValueType::Float8);
		assert_eq!(view.get_value(0), Value::float8(10.5));
		assert_eq!(view.get_value(1), Value::float8(2.5));
		assert_eq!(view.get_value(2), Value::float8(3.5));
		assert_eq!(view.get_value(3), Value::float8(40.5));
	}

	#[test]
	fn a_multi_column_value_in_a_single_column_slot_is_rejected() {
		// Taking the first column and discarding the rest loses data with no signal; map, extend
		// and patch all reach a value expression through this path.
		let base = EvalContext::testing();
		let ctx = base.with_eval(four_row_ctx(vec![ints("a", [1, 2, 3, 4]), ints("b", [10, 20, 30, 40])]), 4);

		let err = evaluate(&ctx, &multi(["a", "b"])).expect_err("a two column value must not be truncated");

		assert_eq!(err.0.code, "RUNTIME_010");
	}

	#[test]
	fn combining_boolean_columns_of_different_lengths_is_an_error() {
		// A length mismatch in one batch must be an error, never padded none rows or an out of range read.
		let left = factory::bool("l", [true, true, false]);
		let right = factory::bool("r", [true, false]);

		let result = combine_bool_columns(left, right, Fragment::internal("and"), |l, r| l && r);

		assert!(
			result.is_err(),
			"a right side shorter than the left must be an error, got {:?}",
			result.map(|c| c.1)
		);
	}
}
