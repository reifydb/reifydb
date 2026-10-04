// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, result::Result as StdResult, sync::Arc};

use arrow_array::{
	Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Datum, FixedSizeBinaryArray, PrimitiveArray, Scalar,
	types::{
		Decimal128Type, Decimal256Type, Float64Type, Int16Type, Int32Type, Int64Type, UInt16Type, UInt32Type,
		UInt64Type,
	},
};
use arrow_buffer::i256;
use arrow_ord::cmp;
use arrow_schema::FieldRef;
use reifydb_core::{
	error::CoreError,
	value::column::{factory::none_typed, nulls::split_nulls},
};
use reifydb_value::{
	error::{Diagnostic, Error, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	return_error,
	value::{
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::{
			decimal_array::{self, DECIMAL128_MAX_PRECISION, DecimalView},
			fixed_array,
			wide_int_array::{WideInt, wide_array, wides},
		},
		decimal::unscaled,
		ordered_f64::OrderedF64,
		value_type::ValueType,
	},
};

use super::{logic::bool_column, option::is_all_none};
use crate::Result;

pub trait CompareOp {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray>;
}

pub struct Equal;
pub struct NotEqual;
pub struct GreaterThan;
pub struct GreaterThanEqual;
pub struct LessThan;
pub struct LessThanEqual;

impl CompareOp for Equal {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::eq(left, right))
	}
}

impl CompareOp for NotEqual {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::neq(left, right))
	}
}

impl CompareOp for GreaterThan {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::gt(left, right))
	}
}

impl CompareOp for GreaterThanEqual {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::gt_eq(left, right))
	}
}

impl CompareOp for LessThan {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::lt(left, right))
	}
}

impl CompareOp for LessThanEqual {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::lt_eq(left, right))
	}
}

fn arrow_result<E: Display>(result: StdResult<BooleanArray, E>) -> Result<BooleanArray> {
	Ok(result.map_err(|err| CoreError::FrameError {
		message: err.to_string(),
	})?)
}

fn signed_rank(ty: &ValueType) -> Option<u8> {
	match ty {
		ValueType::Int1 => Some(1),
		ValueType::Int2 => Some(2),
		ValueType::Int4 => Some(3),
		ValueType::Int8 => Some(4),
		ValueType::Int16 => Some(5),
		_ => None,
	}
}

fn unsigned_rank(ty: &ValueType) -> Option<u8> {
	match ty {
		ValueType::Uint1 => Some(1),
		ValueType::Uint2 => Some(2),
		ValueType::Uint4 => Some(3),
		ValueType::Uint8 => Some(4),
		ValueType::Uint16 => Some(5),
		_ => None,
	}
}

fn signed_of_rank(rank: u8) -> ValueType {
	match rank {
		1 => ValueType::Int1,
		2 => ValueType::Int2,
		3 => ValueType::Int4,
		4 => ValueType::Int8,
		5 => ValueType::Int16,
		_ => ValueType::decimal(Precision::new(39), Scale::new(0)),
	}
}

fn unsigned_of_rank(rank: u8) -> ValueType {
	match rank {
		1 => ValueType::Uint1,
		2 => ValueType::Uint2,
		3 => ValueType::Uint4,
		4 => ValueType::Uint8,
		_ => ValueType::Uint16,
	}
}

fn integer_digits(ty: &ValueType) -> Option<u8> {
	match ty {
		ValueType::Int1 | ValueType::Uint1 => Some(3),
		ValueType::Int2 | ValueType::Uint2 => Some(5),
		ValueType::Int4 | ValueType::Uint4 => Some(10),
		ValueType::Int8 => Some(19),
		ValueType::Uint8 => Some(20),
		ValueType::Int16 | ValueType::Uint16 => Some(39),
		_ => None,
	}
}

pub(crate) fn family_digits(ty: &ValueType) -> Option<(u8, u8)> {
	match ty {
		ValueType::Decimal {
			precision,
			scale,
		} => Some((precision.value() - scale.value(), scale.value())),
		_ => integer_digits(ty).map(|digits| (digits, 0)),
	}
}

pub(crate) fn is_family(ty: &ValueType) -> bool {
	matches!(ty, ValueType::Decimal { .. })
}

pub(crate) fn compare_target(left: &ValueType, right: &ValueType) -> Option<ValueType> {
	if is_family(left) || is_family(right) {
		if left.is_floating_point() || right.is_floating_point() {
			return (left.is_number() && right.is_number()).then_some(ValueType::Float8);
		}
		let ((left_digits, left_scale), (right_digits, right_scale)) =
			(family_digits(left)?, family_digits(right)?);
		let scale = left_scale.max(right_scale);
		let precision = (left_digits.max(right_digits) + scale).min(unscaled::MAX_DIGITS);
		return Some(ValueType::decimal(Precision::new(precision), Scale::new(scale)));
	}
	if left == right {
		return matches!(
			left,
			ValueType::Boolean
				| ValueType::Float4
				| ValueType::Float8
				| ValueType::Int1
				| ValueType::Int2
				| ValueType::Int4
				| ValueType::Int8
				| ValueType::Int16
				| ValueType::Uint1
				| ValueType::Uint2
				| ValueType::Uint4
				| ValueType::Uint8
				| ValueType::Uint16
				| ValueType::Utf8
				| ValueType::Blob
				| ValueType::Date
				| ValueType::DateTime
				| ValueType::Time
				| ValueType::Duration
				| ValueType::Uuid4
				| ValueType::Uuid7
				| ValueType::IdentityId
		)
		.then(|| left.clone());
	}
	if left.is_floating_point() || right.is_floating_point() {
		return (left.is_number() && right.is_number()).then_some(ValueType::Float8);
	}
	match (signed_rank(left), unsigned_rank(left), signed_rank(right), unsigned_rank(right)) {
		(Some(l), _, Some(r), _) => Some(signed_of_rank(l.max(r))),
		(_, Some(l), _, Some(r)) => Some(unsigned_of_rank(l.max(r))),
		(Some(s), _, _, Some(u)) | (_, Some(u), Some(s), _) => Some(signed_of_rank(s.max(u + 1))),
		_ => None,
	}
}

fn wide_unary<W: WideInt, T: ArrowPrimitiveType>(
	array: &FixedSizeBinaryArray,
	f: impl Fn(W) -> T::Native,
) -> PrimitiveArray<T> {
	PrimitiveArray::new(wides::<W>(array).into_iter().map(f).collect(), array.logical_nulls())
}

fn to_wide<T: ArrowPrimitiveType, W: WideInt>(
	array: &PrimitiveArray<T>,
	f: impl Fn(T::Native) -> W,
) -> FixedSizeBinaryArray {
	fixed_array::attach_nulls(wide_array(array.values().iter().map(|&v| f(v))), array.logical_nulls())
}

macro_rules! widen {
	($array:expr, $to:ty, |$v:ident| $conv:expr) => {
		Arc::new($array.unary::<_, $to>(|$v| $conv)) as ArrayRef
	};
}

pub(crate) fn cast_to(column: &(FieldRef, ArrayRef), target: &ValueType) -> Result<ArrayRef> {
	let view = ColumnView::try_from(column)?;
	if view.get_type().inner_type() == target {
		return Ok(column.1.clone());
	}
	Ok(match (target, &view.data) {
		(ValueType::Float8, ViewData::Float4(a)) => widen!(a, Float64Type, |v| OrderedF64::canonical(v as f64)),
		(ValueType::Float8, ViewData::Int1(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Int2(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Int4(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Int8(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Int16(a)) => Arc::new(wide_unary::<i128, Float64Type>(a, |v| v as f64)),
		(ValueType::Float8, ViewData::Uint1(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Uint2(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Uint4(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Uint8(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ViewData::Uint16(a)) => Arc::new(wide_unary::<u128, Float64Type>(a, |v| v as f64)),
		(ValueType::Int2, ViewData::Int1(a)) => widen!(a, Int16Type, |v| v as i16),
		(ValueType::Int2, ViewData::Uint1(a)) => widen!(a, Int16Type, |v| v as i16),
		(ValueType::Int4, ViewData::Int1(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int4, ViewData::Int2(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int4, ViewData::Uint1(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int4, ViewData::Uint2(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int8, ViewData::Int1(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ViewData::Int2(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ViewData::Int4(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ViewData::Uint1(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ViewData::Uint2(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ViewData::Uint4(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Uint2, ViewData::Uint1(a)) => widen!(a, UInt16Type, |v| v as u16),
		(ValueType::Uint4, ViewData::Uint1(a)) => widen!(a, UInt32Type, |v| v as u32),
		(ValueType::Uint4, ViewData::Uint2(a)) => widen!(a, UInt32Type, |v| v as u32),
		(ValueType::Uint8, ViewData::Uint1(a)) => widen!(a, UInt64Type, |v| v as u64),
		(ValueType::Uint8, ViewData::Uint2(a)) => widen!(a, UInt64Type, |v| v as u64),
		(ValueType::Uint8, ViewData::Uint4(a)) => widen!(a, UInt64Type, |v| v as u64),
		(ValueType::Int16, ViewData::Int1(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Int2(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Int4(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Int8(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Uint1(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Uint2(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Uint4(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ViewData::Uint8(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Uint16, ViewData::Uint1(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Uint16, ViewData::Uint2(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Uint16, ViewData::Uint4(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Uint16, ViewData::Uint8(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Float8, ViewData::Decimal(a)) => family_to_float(a),
		(
			ValueType::Decimal {
				precision,
				scale,
			},
			_,
		) => family_array(&column.1, &view, *precision, *scale),
		_ => unreachable!(),
	})
}

fn family_to_float(array: &DecimalView) -> ArrayRef {
	let divisor = 10f64.powi(i32::from(array.scale().value()));
	match array {
		DecimalView::Decimal128(a) => widen!(a, Float64Type, |v| v as f64 / divisor),
		DecimalView::Decimal256(a) => widen!(a, Float64Type, |v| i256_to_f64(v) / divisor),
	}
}

fn i256_to_f64(value: i256) -> f64 {
	match value.to_i128() {
		Some(narrow) => narrow as f64,
		None => {
			let (low, high) = value.to_parts();
			high as f64 * 2f64.powi(128) + low as f64
		}
	}
}

const BEYOND_FAMILY: i256 = unscaled::MAX.wrapping_add(i256::ONE);

fn upscale_or_beyond(value: i256, by: u8) -> i256 {
	unscaled::upscale(value, by).unwrap_or(if value.is_negative() {
		BEYOND_FAMILY.wrapping_neg()
	} else {
		BEYOND_FAMILY
	})
}

fn family_scale(view: &ColumnView) -> u8 {
	match &view.data {
		ViewData::Decimal(a) => a.scale().value(),
		_ => 0,
	}
}

macro_rules! rescale128 {
	($array:expr, $factor:expr) => {
		$array.unary::<_, Decimal128Type>(|v| i128::from(v).wrapping_mul($factor))
	};
}

macro_rules! rescale256 {
	($array:expr, $by:expr, |$v:ident| $wide:expr) => {
		$array.unary::<_, Decimal256Type>(|$v| upscale_or_beyond($wide, $by))
	};
}

fn family_array(column: &ArrayRef, view: &ColumnView, precision: Precision, scale: Scale) -> ArrayRef {
	let data_type = decimal_array::data_type(precision, scale);
	if let ViewData::Decimal(a) = &view.data
		&& a.data_type() == &data_type
	{
		return column.clone();
	}
	let by = scale.value() - family_scale(view);
	if precision.value() <= DECIMAL128_MAX_PRECISION {
		let factor = 10i128.pow(u32::from(by));
		let array = match &view.data {
			ViewData::Int1(a) => rescale128!(a, factor),
			ViewData::Int2(a) => rescale128!(a, factor),
			ViewData::Int4(a) => rescale128!(a, factor),
			ViewData::Int8(a) => rescale128!(a, factor),
			ViewData::Uint1(a) => rescale128!(a, factor),
			ViewData::Uint2(a) => rescale128!(a, factor),
			ViewData::Uint4(a) => rescale128!(a, factor),
			ViewData::Uint8(a) => rescale128!(a, factor),
			ViewData::Decimal(DecimalView::Decimal128(a)) => rescale128!(a, factor),
			_ => unreachable!(),
		};
		Arc::new(array.with_data_type(data_type))
	} else {
		let array = match &view.data {
			ViewData::Int1(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Int2(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Int4(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Int8(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Int16(a) => {
				wide_unary::<i128, Decimal256Type>(a, |v| upscale_or_beyond(i256::from_i128(v), by))
			}
			ViewData::Uint1(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Uint2(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Uint4(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Uint8(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ViewData::Uint16(a) => {
				wide_unary::<u128, Decimal256Type>(a, |v| upscale_or_beyond(i256::from_parts(v, 0), by))
			}
			ViewData::Decimal(DecimalView::Decimal128(a)) => {
				rescale256!(a, by, |v| i256::from_i128(v))
			}
			ViewData::Decimal(DecimalView::Decimal256(a)) => rescale256!(a, by, |v| v),
			_ => unreachable!(),
		};
		Arc::new(array.with_data_type(data_type))
	}
}

pub(crate) fn length_mismatch(left: usize, right: usize, fragment: &Fragment) -> Error {
	TypeError::Runtime {
		kind: RuntimeErrorKind::ColumnLengthMismatch {
			left,
			right,
			fragment: fragment.clone(),
		},
		message: format!("cannot combine a column of {left} rows with a column of {right} rows"),
	}
	.into()
}

pub fn compare_columns<Op: CompareOp>(
	left: &(FieldRef, ArrayRef),
	right: &(FieldRef, ArrayRef),
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, ValueType, ValueType) -> Diagnostic,
) -> Result<(FieldRef, ArrayRef)> {
	let len = match (left.1.len(), right.1.len()) {
		(l, r) if l == r => l,
		(1, r) => r,
		(l, 1) => l,
		(l, r) => return Err(length_mismatch(l, r, &fragment)),
	};
	let (left_data, left_nulls) = split_nulls(left.clone())?;
	let (right_data, right_nulls) = split_nulls(right.clone())?;
	if ColumnView::try_from(left)?.is_untyped_none() || ColumnView::try_from(right)?.is_untyped_none() {
		return Ok(none_typed(fragment.text(), ValueType::Boolean, len));
	}
	let (left_type, right_type) =
		(ColumnView::try_from(&left_data)?.get_type(), ColumnView::try_from(&right_data)?.get_type());
	let Some(target) = compare_target(&left_type, &right_type) else {
		return_error!(error_fn(fragment, left_type, right_type))
	};
	if is_all_none(left_nulls.as_ref()) || is_all_none(right_nulls.as_ref()) {
		return Ok(none_typed(fragment.text(), ValueType::Boolean, len));
	}
	let left_array = cast_to(left, &target)?;
	let right_array = cast_to(right, &target)?;
	let result = kernel_compare::<Op>(left_array, right_array)?;
	Ok(bool_column(fragment.text(), result, left.0.is_nullable() || right.0.is_nullable()))
}

fn kernel_compare<Op: CompareOp>(left_array: ArrayRef, right_array: ArrayRef) -> Result<BooleanArray> {
	Ok(match (left_array.len(), right_array.len()) {
		(1, r) if r != 1 => Op::kernel(&Scalar::new(left_array), &right_array)?,
		(l, 1) if l != 1 => Op::kernel(&left_array, &Scalar::new(right_array))?,
		_ => Op::kernel(&left_array, &right_array)?,
	})
}
