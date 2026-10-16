// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	arith::{
		ArithOp as CoreArithOp,
		add::add_columns,
		arith_target as core_arith_target,
		context::{ArithContext, Overflow},
		div::div_columns,
		family_digits, is_family,
		mul::mul_columns,
		rem::rem_columns,
		sub::sub_columns,
	},
	builder::ColumnBuilder,
	factory::{default_typed, none},
	nulls::split_nulls,
};
use reifydb_routine_abi::{context::FunctionContext, error::RoutineError};
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	constraint::{precision::Precision, scale::Scale},
	container::varlen_array,
	decimal::unscaled,
	value_type::{ValueType, input_types::InputTypes},
};

use crate::function::support::coerce::{
	CoerceMode, all_rows_none, bare_type, coerce_column, kernel_type, promote_pair,
};

#[derive(Debug, Clone, Copy)]
pub enum BasicStrategy {
	Default,
	Saturate,
	Wrap,
	Zero,
	None,
}

#[derive(Debug, Clone, Copy)]
pub enum ArithShape {
	Two,
	Fallback,
	Strict,
}

pub(crate) fn ensure_numeric(
	ctx: &mut FunctionContext,
	data: &ColumnView,
	argument_index: usize,
) -> Result<(), RoutineError> {
	let actual = bare_type(data);
	if !actual.is_number() && actual != ValueType::Any {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual,
		});
	}
	Ok(())
}

fn unwrap_option(ty: ValueType) -> ValueType {
	match ty {
		ValueType::Option(inner) => *inner,
		other => other,
	}
}

pub(crate) fn arith_target(op: CoreArithOp, a: &ValueType, b: &ValueType, fallback: Option<&ValueType>) -> ValueType {
	let promoted = promote_pair(a.clone(), b.clone());
	if promoted == ValueType::Any {
		return ValueType::Any;
	}
	if *a == ValueType::Any || *b == ValueType::Any {
		return promoted;
	}
	let target = core_arith_target(op, a.clone(), b.clone());
	let Some(fallback) = fallback.map(|fallback| unwrap_option(fallback.clone())) else {
		return target;
	};
	if !is_family(&target) {
		return target;
	}
	let digits = |ty: &ValueType| {
		if ty.is_floating_point() {
			family_digits(&ValueType::DECIMAL)
		} else {
			family_digits(ty)
		}
	};
	let (Some((target_digits, target_scale)), Some((fallback_digits, fallback_scale))) =
		(digits(&target), digits(&fallback))
	else {
		return target;
	};
	let max = unscaled::MAX_DIGITS;
	let scale = target_scale.max(fallback_scale).min(max);
	let integer = target_digits.max(fallback_digits);
	ValueType::decimal(Precision::new(integer.saturating_add(scale).clamp(1, max)), Scale::new(scale))
}

pub(crate) fn arith_return_type_of(args: &[(ValueType, bool)], op: CoreArithOp, shape: ArithShape) -> ValueType {
	let (Some((a, a_nullable)), Some((b, b_nullable))) = (args.first(), args.get(1)) else {
		return ValueType::Float8;
	};
	let fallback = match shape {
		ArithShape::Fallback => args.get(2),
		_ => None,
	};
	let fallback_type = fallback.map(|(value_type, _)| kernel_type(value_type));
	let target = arith_target(op, &kernel_type(a), &kernel_type(b), fallback_type.as_ref());
	if *a_nullable || *b_nullable || target == ValueType::Any {
		ValueType::Option(Box::new(target))
	} else {
		target
	}
}

fn make_strict_error(ctx: &FunctionContext, msg_col: &ColumnView, i: usize) -> RoutineError {
	let reason = match &msg_col.data {
		ViewData::Utf8 {
			container,
			..
		} => varlen_array::get(*container, i).unwrap_or("overflow").to_string(),
		_ => "overflow".to_string(),
	};
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

pub fn dispatch_two(
	ctx: &mut FunctionContext,
	args: &[(FieldRef, ArrayRef)],
	op: CoreArithOp,
	strategy: BasicStrategy,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	if let Some(early) = preamble(ctx, &args[0], &args[1])? {
		return Ok(early);
	}
	match strategy {
		BasicStrategy::Default => kernel(ctx, &args[0], &args[1], op, Overflow::Error),
		BasicStrategy::None => kernel(ctx, &args[0], &args[1], op, Overflow::None),
		BasicStrategy::Saturate => kernel(ctx, &args[0], &args[1], op, Overflow::Saturate),
		BasicStrategy::Wrap => kernel(ctx, &args[0], &args[1], op, Overflow::Wrap),
		BasicStrategy::Zero => {
			let result = kernel(ctx, &args[0], &args[1], op, Overflow::None)?;
			let view = ColumnView::try_from(&result)?;
			let (zero_field, zero_array) = default_typed("zero", view.get_type().inner_type().clone(), 1);
			let zero = ColumnView::try_from((&zero_array, zero_field.as_ref()))?.get_value(0);
			refill(ctx, &args[0], &args[1], &result, |_| Ok(Some(zero.clone())))
		}
	}
}

pub fn dispatch_fallback(
	ctx: &mut FunctionContext,
	args: &[(FieldRef, ArrayRef)],
	op: CoreArithOp,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let (d_bare, _) = split_nulls(args[2].clone())?;
	ensure_numeric(ctx, &ColumnView::try_from(&d_bare)?, 2)?;
	if let Some(early) = preamble(ctx, &args[0], &args[1])? {
		return Ok(early);
	}
	let result = kernel(ctx, &args[0], &args[1], op, Overflow::None)?;
	let bare = |column: &(FieldRef, ArrayRef)| -> Result<ValueType, RoutineError> {
		let (bare, _) = split_nulls(column.clone())?;
		Ok(bare_type(&ColumnView::try_from(&bare)?))
	};
	let target = arith_target(op, &bare(&args[0])?, &bare(&args[1])?, Some(&bare(&args[2])?));
	let result = match ColumnView::try_from(&result)?.get_type().inner_type() == &target {
		true => result,
		false => coerce_column(ctx, &ColumnView::try_from(&result)?, target.clone(), CoerceMode::Error)?,
	};
	let fallback = coerce_column(ctx, &ColumnView::try_from(&args[2])?, target, CoerceMode::Error)?;
	let fallback = ColumnView::try_from(&fallback)?;
	refill(ctx, &args[0], &args[1], &result, |row| Ok(fallback.is_defined(row).then(|| fallback.get_value(row))))
}

pub fn dispatch_strict(
	ctx: &mut FunctionContext,
	args: &[(FieldRef, ArrayRef)],
	op: CoreArithOp,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let (msg_bare, _) = split_nulls(args[2].clone())?;
	let msg_data = ColumnView::try_from(&msg_bare)?;
	let actual = bare_type(&msg_data);
	if actual != ValueType::Utf8 {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 2,
			expected: vec![ValueType::Utf8],
			actual,
		});
	}
	if let Some(early) = preamble(ctx, &args[0], &args[1])? {
		return Ok(early);
	}
	let result = kernel(ctx, &args[0], &args[1], op, Overflow::None)?;
	refill(ctx, &args[0], &args[1], &result, |row| Err(make_strict_error(ctx, &msg_data, row)))
}

fn preamble(
	ctx: &mut FunctionContext,
	a_col: &(FieldRef, ArrayRef),
	b_col: &(FieldRef, ArrayRef),
) -> Result<Option<(FieldRef, ArrayRef)>, RoutineError> {
	let (a_bare, _) = split_nulls(a_col.clone())?;
	let (b_bare, _) = split_nulls(b_col.clone())?;
	let a_data = ColumnView::try_from(&a_bare)?;
	let b_data = ColumnView::try_from(&b_bare)?;
	ensure_numeric(ctx, &a_data, 0)?;
	ensure_numeric(ctx, &b_data, 1)?;
	if promote_pair(bare_type(&a_data), bare_type(&b_data)) == ValueType::Any {
		if all_rows_none(&ColumnView::try_from(a_col)?) && all_rows_none(&ColumnView::try_from(b_col)?) {
			return Ok(Some(none(ctx.fragment.text(), a_data.len())));
		}
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 0,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual: ValueType::Any,
		});
	}
	Ok(None)
}

fn kernel(
	ctx: &FunctionContext,
	a: &(FieldRef, ArrayRef),
	b: &(FieldRef, ArrayRef),
	op: CoreArithOp,
	overflow: Overflow,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let arith = ArithContext {
		target: None,
		overflow,
	};
	let fragment = &ctx.fragment;
	let lazy = || fragment.clone();
	let result = match op {
		CoreArithOp::Add => add_columns(&arith, a, b, lazy),
		CoreArithOp::Sub => sub_columns(&arith, a, b, lazy),
		CoreArithOp::Mul => mul_columns(&arith, a, b, lazy),
		CoreArithOp::Div => div_columns(&arith, a, b, lazy),
		CoreArithOp::Rem => rem_columns(&arith, a, b, lazy),
	};
	Ok(result?)
}

fn refill(
	ctx: &FunctionContext,
	a: &(FieldRef, ArrayRef),
	b: &(FieldRef, ArrayRef),
	result: &(FieldRef, ArrayRef),
	mut overflowed: impl FnMut(usize) -> Result<Option<Value>, RoutineError>,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let view = ColumnView::try_from(result)?;
	let overflow_rows: Vec<usize> = (0..view.len())
		.filter(|&row| !view.is_defined(row) && a.1.is_valid(row) && b.1.is_valid(row))
		.collect();
	if overflow_rows.is_empty() {
		return Ok(result.clone());
	}
	let target = view.get_type().inner_type().clone();
	let mut builder = ColumnBuilder::with_capacity(target.clone(), view.len());
	let mut next = overflow_rows.iter().peekable();
	for row in 0..view.len() {
		if next.peek() == Some(&&row) {
			next.next();
			match overflowed(row)? {
				Some(value) => builder.push_value(value),
				None => builder.push_none(),
			}
		} else if view.is_defined(row) {
			builder.push_value(view.get_value(row));
		} else {
			builder.push_none();
		}
	}
	let built = builder.finish(ctx.fragment.text());
	if ColumnView::try_from(&built)?.get_type().inner_type() == &target {
		return Ok(built);
	}
	coerce_column(ctx, &ColumnView::try_from(&built)?, target, CoerceMode::Error)
}
