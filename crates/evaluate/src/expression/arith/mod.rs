// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

macro_rules! dispatch_arith {

	(
		$left:expr, $right:expr;
		fixed: $fh:ident, arb: $ah:ident ($ctx:expr, $target:expr, $fragment:expr);
		$($extra:tt)*
	) => {
		dispatch_arith!(@rows
			($left, $right) $fh $ah ($ctx, $target, $fragment)
			[Float4 Float8 Int1 Int2 Int4 Int8 Int16 Uint1 Uint2 Uint4 Uint8 Uint16]
			{$($extra)*}
			{}
		)
	};


	(@rows
		($left:expr, $right:expr) $fh:ident $ah:ident ($ctx:expr, $target:expr, $fragment:expr)
		[$L:ident $($rest:ident)*]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		dispatch_arith!(@rows
			($left, $right) $fh $ah ($ctx, $target, $fragment)
			[$($rest)*]
			{$($extra)*}
			{
				$($acc)*
				(ColumnBuffer::$L(l), ColumnBuffer::Float4(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Float8(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int1(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int2(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int4(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int8(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int16(r)) => $fh($ctx, dispatch_arith!(@values $L l), &wides::<i128>(r), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint1(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint2(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint4(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint8(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint16(r)) => $fh($ctx, dispatch_arith!(@values $L l), &wides::<u128>(r), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Decimal(r)) => $ah($ctx, dispatch_arith!(@values $L l), &decimals(r), $target, $fragment),
			}
		)
	};


	(@rows
		($left:expr, $right:expr) $fh:ident $ah:ident ($ctx:expr, $target:expr, $fragment:expr)
		[]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		match ($left, $right) {

			$($acc)*




			(ColumnBuffer::Decimal(l), ColumnBuffer::Float4(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Float8(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int1(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int2(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int4(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int8(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int16(r)) => $ah($ctx, &decimals(l), &wides::<i128>(r), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint1(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint2(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint4(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint8(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint16(r)) => $ah($ctx, &decimals(l), &wides::<u128>(r), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Decimal(r)) => $ah($ctx, &decimals(l), &decimals(r), $target, $fragment),


			$($extra)*
		}
	};


	(@values Int16 $array:ident) => {
		&wides::<i128>($array)
	};


	(@values Uint16 $array:ident) => {
		&wides::<u128>($array)
	};


	(@values $L:ident $array:ident) => {
		$array.values()
	};
}

pub mod add;
pub mod div;
pub mod mul;
pub mod rem;
pub mod sub;

use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	decimal::unscaled::MAX_DIGITS,
	value_type::ValueType,
};

use crate::expression::compare::{family_digits, is_family};

#[derive(Clone, Copy)]
pub(crate) enum ArithOp {
	Add,
	Sub,
	Mul,
	Div,
	Rem,
}

fn arith_digits(ty: &ValueType) -> Option<(u8, u8)> {
	if ty.is_floating_point() {
		return family_digits(&ValueType::DECIMAL);
	}
	family_digits(ty)
}

pub(crate) fn arith_target(op: ArithOp, left: ValueType, right: ValueType) -> ValueType {
	if !(is_family(&left) || is_family(&right)) {
		return ValueType::promote(left, right);
	}
	let (Some((left_digits, left_scale)), Some((right_digits, right_scale))) =
		(arith_digits(&left), arith_digits(&right))
	else {
		return ValueType::promote(left, right);
	};
	let (digits, scale) = match op {
		ArithOp::Add | ArithOp::Sub => (left_digits.max(right_digits) + 1, left_scale.max(right_scale)),
		ArithOp::Mul => {
			let (digits, scale) = (left_digits + right_digits, left_scale + right_scale);
			if digits.saturating_add(scale) > MAX_DIGITS {
				(digits, scale.min(6).max(MAX_DIGITS.saturating_sub(digits)))
			} else {
				(digits, scale)
			}
		}
		ArithOp::Div => (left_digits + right_scale, left_scale.max(right_scale).max(6)),
		ArithOp::Rem => (left_digits.min(right_digits), left_scale.max(right_scale)),
	};
	let scale = scale.min(MAX_DIGITS);
	let precision = Precision::new(digits.saturating_add(scale).clamp(1, MAX_DIGITS));
	ValueType::decimal(precision, Scale::new(scale))
}
