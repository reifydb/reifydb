// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, result::Result as StdResult, sync::Arc};

use arrow_array::{
	Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Datum, FixedSizeBinaryArray, PrimitiveArray, Scalar,
	cast::AsArray,
	types::{
		Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, UInt16Type,
		UInt32Type, UInt64Type,
	},
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, i256};
use arrow_ord::cmp;
use reifydb_core::{
	error::CoreError,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_value::{
	error::{Diagnostic, Error, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	return_error,
	value::{
		constraint::{precision::Precision, scale::Scale},
		container::{
			decimal_array::{self, DECIMAL128_MAX_PRECISION, DecimalArray},
			fixed_array,
			wide_int_array::{WideInt, wide_array, wides},
		},
		decimal::unscaled,
		value_type::ValueType,
	},
};

use super::option::{is_all_none, is_untyped_none};
use crate::Result;

pub trait CompareOp {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray>;
	fn float(left: f64, right: f64) -> bool;
}

fn float_eq(left: f64, right: f64) -> bool {
	left == right || (left.is_nan() && right.is_nan())
}

fn float_lt(left: f64, right: f64) -> bool {
	left < right || (right.is_nan() && !left.is_nan())
}

fn float_le(left: f64, right: f64) -> bool {
	left <= right || right.is_nan()
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

	fn float(left: f64, right: f64) -> bool {
		float_eq(left, right)
	}
}

impl CompareOp for NotEqual {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::neq(left, right))
	}

	fn float(left: f64, right: f64) -> bool {
		!float_eq(left, right)
	}
}

impl CompareOp for GreaterThan {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::gt(left, right))
	}

	fn float(left: f64, right: f64) -> bool {
		float_lt(right, left)
	}
}

impl CompareOp for GreaterThanEqual {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::gt_eq(left, right))
	}

	fn float(left: f64, right: f64) -> bool {
		float_le(right, left)
	}
}

impl CompareOp for LessThan {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::lt(left, right))
	}

	fn float(left: f64, right: f64) -> bool {
		float_lt(left, right)
	}
}

impl CompareOp for LessThanEqual {
	fn kernel(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray> {
		arrow_result(cmp::lt_eq(left, right))
	}

	fn float(left: f64, right: f64) -> bool {
		float_le(left, right)
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

fn compare_target(left: &ValueType, right: &ValueType) -> Option<ValueType> {
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
	PrimitiveArray::new(wides::<W>(array).into_iter().map(f).collect(), array.nulls().cloned())
}

fn to_wide<T: ArrowPrimitiveType, W: WideInt>(
	array: &PrimitiveArray<T>,
	f: impl Fn(T::Native) -> W,
) -> FixedSizeBinaryArray {
	fixed_array::attach_nulls(wide_array(array.values().iter().map(|&v| f(v))), array.nulls().cloned())
}

macro_rules! widen {
	($array:expr, $to:ty, |$v:ident| $conv:expr) => {
		Arc::new($array.unary::<_, $to>(|$v| $conv)) as ArrayRef
	};
}

fn cast_to(column: &ColumnBuffer, target: &ValueType) -> ArrayRef {
	if column.get_type().inner_type() == target {
		return column.to_array_ref();
	}
	match (target, column) {
		(ValueType::Float8, ColumnBuffer::Float4(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Int1(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Int2(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Int4(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Int8(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Int16(a)) => {
			Arc::new(wide_unary::<i128, Float64Type>(a, |v| v as f64))
		}
		(ValueType::Float8, ColumnBuffer::Uint1(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Uint2(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Uint4(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Uint8(a)) => widen!(a, Float64Type, |v| v as f64),
		(ValueType::Float8, ColumnBuffer::Uint16(a)) => {
			Arc::new(wide_unary::<u128, Float64Type>(a, |v| v as f64))
		}
		(ValueType::Int2, ColumnBuffer::Int1(a)) => widen!(a, Int16Type, |v| v as i16),
		(ValueType::Int2, ColumnBuffer::Uint1(a)) => widen!(a, Int16Type, |v| v as i16),
		(ValueType::Int4, ColumnBuffer::Int1(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int4, ColumnBuffer::Int2(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int4, ColumnBuffer::Uint1(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int4, ColumnBuffer::Uint2(a)) => widen!(a, Int32Type, |v| v as i32),
		(ValueType::Int8, ColumnBuffer::Int1(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ColumnBuffer::Int2(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ColumnBuffer::Int4(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ColumnBuffer::Uint1(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ColumnBuffer::Uint2(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Int8, ColumnBuffer::Uint4(a)) => widen!(a, Int64Type, |v| v as i64),
		(ValueType::Uint2, ColumnBuffer::Uint1(a)) => widen!(a, UInt16Type, |v| v as u16),
		(ValueType::Uint4, ColumnBuffer::Uint1(a)) => widen!(a, UInt32Type, |v| v as u32),
		(ValueType::Uint4, ColumnBuffer::Uint2(a)) => widen!(a, UInt32Type, |v| v as u32),
		(ValueType::Uint8, ColumnBuffer::Uint1(a)) => widen!(a, UInt64Type, |v| v as u64),
		(ValueType::Uint8, ColumnBuffer::Uint2(a)) => widen!(a, UInt64Type, |v| v as u64),
		(ValueType::Uint8, ColumnBuffer::Uint4(a)) => widen!(a, UInt64Type, |v| v as u64),
		(ValueType::Int16, ColumnBuffer::Int1(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Int2(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Int4(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Int8(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Uint1(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Uint2(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Uint4(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Int16, ColumnBuffer::Uint8(a)) => Arc::new(to_wide(a, |v| v as i128)),
		(ValueType::Uint16, ColumnBuffer::Uint1(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Uint16, ColumnBuffer::Uint2(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Uint16, ColumnBuffer::Uint4(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Uint16, ColumnBuffer::Uint8(a)) => Arc::new(to_wide(a, |v| v as u128)),
		(ValueType::Float8, ColumnBuffer::Decimal(a)) => family_to_float(a),
		(
			ValueType::Decimal {
				precision,
				scale,
			},
			_,
		) => family_array(column, *precision, *scale),
		_ => unreachable!(),
	}
}

fn family_to_float(array: &DecimalArray) -> ArrayRef {
	let divisor = 10f64.powi(i32::from(array.scale().value()));
	match array {
		DecimalArray::Decimal128(a) => widen!(a, Float64Type, |v| v as f64 / divisor),
		DecimalArray::Decimal256(a) => widen!(a, Float64Type, |v| i256_to_f64(v) / divisor),
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

fn family_scale(column: &ColumnBuffer) -> u8 {
	match column {
		ColumnBuffer::Decimal(a) => a.scale().value(),
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

fn family_array(column: &ColumnBuffer, precision: Precision, scale: Scale) -> ArrayRef {
	let data_type = decimal_array::data_type(precision, scale);
	if let ColumnBuffer::Decimal(a) = column
		&& a.data_type() == &data_type
	{
		return column.to_array_ref();
	}
	let by = scale.value() - family_scale(column);
	if precision.value() <= DECIMAL128_MAX_PRECISION {
		let factor = 10i128.pow(u32::from(by));
		let array = match column {
			ColumnBuffer::Int1(a) => rescale128!(a, factor),
			ColumnBuffer::Int2(a) => rescale128!(a, factor),
			ColumnBuffer::Int4(a) => rescale128!(a, factor),
			ColumnBuffer::Int8(a) => rescale128!(a, factor),
			ColumnBuffer::Uint1(a) => rescale128!(a, factor),
			ColumnBuffer::Uint2(a) => rescale128!(a, factor),
			ColumnBuffer::Uint4(a) => rescale128!(a, factor),
			ColumnBuffer::Uint8(a) => rescale128!(a, factor),
			ColumnBuffer::Decimal(DecimalArray::Decimal128(a)) => rescale128!(a, factor),
			_ => unreachable!(),
		};
		Arc::new(array.with_data_type(data_type))
	} else {
		let array = match column {
			ColumnBuffer::Int1(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Int2(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Int4(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Int8(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Int16(a) => {
				wide_unary::<i128, Decimal256Type>(a, |v| upscale_or_beyond(i256::from_i128(v), by))
			}
			ColumnBuffer::Uint1(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Uint2(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Uint4(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Uint8(a) => rescale256!(a, by, |v| i256::from_i128(i128::from(v))),
			ColumnBuffer::Uint16(a) => {
				wide_unary::<u128, Decimal256Type>(a, |v| upscale_or_beyond(i256::from_parts(v, 0), by))
			}
			ColumnBuffer::Decimal(DecimalArray::Decimal128(a)) => {
				rescale256!(a, by, |v| i256::from_i128(v))
			}
			ColumnBuffer::Decimal(DecimalArray::Decimal256(a)) => rescale256!(a, by, |v| v),
			_ => unreachable!(),
		};
		Arc::new(array.with_data_type(data_type))
	}
}

fn compare_floats<Op: CompareOp, T: ArrowPrimitiveType>(
	left: &PrimitiveArray<T>,
	right: &PrimitiveArray<T>,
	len: usize,
) -> BooleanArray
where
	T::Native: Into<f64>,
{
	let (l, r) = (left.values(), right.values());
	let (values, nulls) = match (l.len(), r.len()) {
		(1, n) if n != 1 => {
			let fixed = l[0].into();
			(pack_each(r, len, |v| Op::float(fixed, v.into())), right.nulls().cloned())
		}
		(n, 1) if n != 1 => {
			let fixed = r[0].into();
			(pack_each(l, len, |v| Op::float(v.into(), fixed)), left.nulls().cloned())
		}
		_ => (
			pack_pairs(l, r, len, |a, b| Op::float(a.into(), b.into())),
			NullBuffer::union(left.nulls(), right.nulls()),
		),
	};
	BooleanArray::new(values, nulls)
}

fn pack_each<T: Copy>(values: &[T], len: usize, test: impl Fn(T) -> bool) -> BooleanBuffer {
	let mut words = Vec::with_capacity(len.div_ceil(64));
	for chunk in values.chunks(64) {
		let mut word = 0u64;
		for (bit, value) in chunk.iter().enumerate() {
			word |= u64::from(test(*value)) << bit;
		}
		words.push(word);
	}
	BooleanBuffer::new(Buffer::from_vec(words), 0, len)
}

fn pack_pairs<T: Copy>(left: &[T], right: &[T], len: usize, test: impl Fn(T, T) -> bool) -> BooleanBuffer {
	let mut words = Vec::with_capacity(len.div_ceil(64));
	for (left_chunk, right_chunk) in left.chunks(64).zip(right.chunks(64)) {
		let mut word = 0u64;
		for (bit, (l, r)) in left_chunk.iter().zip(right_chunk).enumerate() {
			word |= u64::from(test(*l, *r)) << bit;
		}
		words.push(word);
	}
	BooleanBuffer::new(Buffer::from_vec(words), 0, len)
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
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: Fragment,
	error_fn: impl FnOnce(Fragment, ValueType, ValueType) -> Diagnostic,
) -> Result<ColumnWithName> {
	let len = match (left.data().len(), right.data().len()) {
		(l, r) if l == r => l,
		(1, r) => r,
		(l, 1) => l,
		(l, r) => return Err(length_mismatch(l, r, &fragment)),
	};
	let (left_data, left_nulls) = left.data().clone().split_nulls();
	let (right_data, right_nulls) = right.data().clone().split_nulls();
	if is_untyped_none(&left_data, left_nulls.as_ref()) || is_untyped_none(&right_data, right_nulls.as_ref()) {
		return Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(ValueType::Boolean, len)));
	}
	let (left_type, right_type) = (left_data.get_type(), right_data.get_type());
	let Some(target) = compare_target(&left_type, &right_type) else {
		return_error!(error_fn(fragment, left_type, right_type))
	};
	if is_all_none(left_nulls.as_ref()) || is_all_none(right_nulls.as_ref()) {
		return Ok(ColumnWithName::new(fragment, ColumnBuffer::none_typed(ValueType::Boolean, len)));
	}
	let left_array = cast_to(left.data(), &target);
	let right_array = cast_to(right.data(), &target);
	let result = match target {
		ValueType::Float4 => {
			compare_floats::<Op, Float32Type>(left_array.as_primitive(), right_array.as_primitive(), len)
		}
		ValueType::Float8 => {
			compare_floats::<Op, Float64Type>(left_array.as_primitive(), right_array.as_primitive(), len)
		}
		_ => kernel_compare::<Op>(left_array, right_array)?,
	};
	Ok(ColumnWithName::new(Fragment::internal(fragment.text()), ColumnBuffer::Bool(result)))
}

fn kernel_compare<Op: CompareOp>(left_array: ArrayRef, right_array: ArrayRef) -> Result<BooleanArray> {
	Ok(match (left_array.len(), right_array.len()) {
		(1, r) if r != 1 => Op::kernel(&Scalar::new(left_array), &right_array)?,
		(l, 1) if l != 1 => Op::kernel(&left_array, &Scalar::new(right_array))?,
		_ => Op::kernel(&left_array, &right_array)?,
	})
}
