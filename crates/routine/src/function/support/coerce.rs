// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, result::Result as StdResult};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::cast::{
	cast_column_data,
	convert::{Convert, TargetConvert},
};
use reifydb_routine_abi::{context::FunctionContext, error::RoutineError};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		container::wide_int_array,
		number::safe::convert::SafeConvert,
		value_type::{ValueType, get::GetType},
	},
};

pub(crate) fn read_i64(function: &Fragment, data: &ColumnView, row: usize) -> StdResult<Option<i64>, RoutineError> {
	narrow(function, data, row, ValueType::Int8, |value| i64::try_from(value).ok())
}

pub(crate) fn read_i32(function: &Fragment, data: &ColumnView, row: usize) -> StdResult<Option<i32>, RoutineError> {
	narrow(function, data, row, ValueType::Int4, |value| i32::try_from(value).ok())
}

fn narrow<T>(
	function: &Fragment,
	data: &ColumnView,
	row: usize,
	target: ValueType,
	fit: impl Fn(i128) -> Option<T>,
) -> StdResult<Option<T>, RoutineError> {
	if let ViewData::Uint16(container) = &data.data {
		return match wide_int_array::wide_at::<u128>(container, row) {
			None => Ok(None),
			Some(value) => i128::try_from(value)
				.ok()
				.and_then(&fit)
				.map(Some)
				.ok_or_else(|| out_of_range(function, value, target)),
		};
	}
	match wide_at(data, row) {
		None => Ok(None),
		Some(value) => fit(value).map(Some).ok_or_else(|| out_of_range(function, value, target)),
	}
}

fn wide_at(data: &ColumnView, row: usize) -> Option<i128> {
	match &data.data {
		ViewData::Int1(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Int2(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Int4(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Int8(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Int16(c) => wide_int_array::wide_at::<i128>(c, row),
		ViewData::Uint1(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Uint2(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Uint4(c) => c.values().get(row).map(|&v| v as i128),
		ViewData::Uint8(c) => c.values().get(row).map(|&v| v as i128),
		_ => None,
	}
}

fn out_of_range(function: &Fragment, value: impl Display, target: ValueType) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: function.clone(),
		reason: format!("{value} is out of range for {target}"),
	}
}

#[derive(Clone, Copy)]
pub(crate) enum CoerceMode {
	Error,
	None,
}

#[derive(Clone, Copy)]
pub(crate) struct NoneConvert;

impl Convert for NoneConvert {
	fn convert<From, To>(&self, from: From, _fragment: impl Into<Fragment>) -> Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		Ok(from.checked_convert())
	}
}

pub(crate) fn coerce_column(
	ctx: &FunctionContext,
	data: &ColumnView,
	target: ValueType,
	mode: CoerceMode,
) -> StdResult<(FieldRef, ArrayRef), RoutineError> {
	let fragment = &ctx.fragment;
	let cast = match mode {
		CoerceMode::Error => cast_column_data(
			TargetConvert {
				target: None,
			},
			data,
			target,
			fragment,
		)?,
		CoerceMode::None => cast_column_data(NoneConvert, data, target, fragment)?,
	};
	Ok(cast)
}

pub(crate) fn all_rows_none(col: &ColumnView) -> bool {
	(0..col.len()).all(|i| !col.is_defined(i))
}

pub(crate) fn bare_type(data: &ColumnView) -> ValueType {
	match data.is_none() {
		true => ValueType::Any,
		false => data.get_type(),
	}
}

pub(crate) fn promote_pair(left: ValueType, right: ValueType) -> ValueType {
	match (left, right) {
		(ValueType::Any, other) => other,
		(other, ValueType::Any) => other,
		(left, right) => ValueType::promote(left, right),
	}
}

pub(crate) fn promote_all(types: impl IntoIterator<Item = ValueType>) -> ValueType {
	types.into_iter().reduce(promote_pair).unwrap_or(ValueType::Float8)
}

#[cfg(test)]
mod tests {
	use std::sync::LazyLock;

	use arrow_buffer::{BooleanBuffer, NullBuffer};
	use reifydb_core::value::column::{
		cast::convert::{Convert, TargetConvert},
		factory::int2,
		nulls::with_nulls,
	};
	use reifydb_routine_abi::context::FunctionContext;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_value::{
		error::IntoDiagnostic,
		fragment::Fragment,
		value::{column_view::ColumnView, identity::IdentityId, value_type::ValueType},
	};

	use super::{CoerceMode, NoneConvert, coerce_column, promote_all};

	fn ctx() -> FunctionContext<'static> {
		static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
		FunctionContext {
			fragment: Fragment::internal("coerce_test"),
			identity: IdentityId::root(),
			row_count: 0,
			runtime_context: &RUNTIME,
		}
	}

	#[test]
	fn none_policy_matches_targetconvert_none_arm() {
		// A checked_convert failure must become Ok(None) here, not an error.
		let out: Option<i8> = NoneConvert.convert(300i16, Fragment::internal("300")).unwrap();
		assert_eq!(out, None);
		let out: Option<i8> = NoneConvert.convert(100i16, Fragment::internal("100")).unwrap();
		assert_eq!(out, Some(100));
		// TargetConvert with the default (Error) mode errors on the same input.
		let err = TargetConvert {
			target: None,
		}
		.convert::<i16, i8>(300i16, Fragment::internal("300"));
		assert!(err.is_err());
	}

	#[test]
	fn error_policy_raises_number_out_of_range() {
		// Out-of-range must surface as the house cast diagnostic, not a generic failure.
		let ctx = ctx();
		let data = int2("value", [300]);
		let input = ColumnView::try_from(&data).unwrap();
		let err = coerce_column(&ctx, &input, ValueType::Int1, CoerceMode::Error).unwrap_err();
		assert_eq!(err.into_diagnostic().code, "NUMBER_002");
	}

	#[test]
	fn none_policy_turns_overflow_into_none() {
		// The same input the Error mode rejects must become an undefined row here.
		let ctx = ctx();
		let data = int2("value", [300, 100]);
		let input = ColumnView::try_from(&data).unwrap();
		let cast = coerce_column(&ctx, &input, ValueType::Int1, CoerceMode::None).unwrap();
		let view = ColumnView::try_from(&cast).unwrap();
		assert!(!view.is_defined(0));
		assert!(view.is_defined(1));
	}

	#[test]
	fn option_shape_and_nones_are_preserved() {
		// Coercion must not flatten Option-shaped input or drop its per-row nones.
		let ctx = ctx();
		let inner = int2("value", [1, 2, 3]);
		let data = with_nulls(inner, NullBuffer::new(BooleanBuffer::from(vec![true, false, true]))).unwrap();
		let input = ColumnView::try_from(&data).unwrap();
		let cast = coerce_column(&ctx, &input, ValueType::Int4, CoerceMode::Error).unwrap();
		let view = ColumnView::try_from(&cast).unwrap();
		assert_eq!(view.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
		assert!(view.is_defined(0));
		assert!(!view.is_defined(1));
		assert!(view.is_defined(2));
	}

	#[test]
	fn promote_all_folds_canonically() {
		assert_eq!(
			promote_all([ValueType::Int1, ValueType::Int1]),
			ValueType::promote(ValueType::Int1, ValueType::Int1)
		);
		assert_eq!(promote_all([ValueType::Float4, ValueType::Float8]), ValueType::Float8);
		assert_eq!(promote_all(Vec::<ValueType>::new()), ValueType::Float8);
	}
}
