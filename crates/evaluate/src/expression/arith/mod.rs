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
				(ColumnBuffer::$L(l), ColumnBuffer::Int16(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint1(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint2(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint4(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint8(r)) => $fh($ctx, dispatch_arith!(@values $L l), r.values(), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint16(r)) => $fh($ctx, dispatch_arith!(@values $L l), &u128s(r), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int(r)) => $ah($ctx, dispatch_arith!(@values $L l), &ints(r), $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint(r)) => $ah($ctx, dispatch_arith!(@values $L l), &uints(r), $target, $fragment),
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


			(ColumnBuffer::Int(l), ColumnBuffer::Float4(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Float8(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Int1(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Int2(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Int4(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Int8(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Int16(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Uint1(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Uint2(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Uint4(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Uint8(r)) => $ah($ctx, &ints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Uint16(r)) => $ah($ctx, &ints(l), &u128s(r), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Int(r)) => $ah($ctx, &ints(l), &ints(r), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Uint(r)) => $ah($ctx, &ints(l), &uints(r), $target, $fragment),
			(ColumnBuffer::Int(l), ColumnBuffer::Decimal(r)) => $ah($ctx, &ints(l), &decimals(r), $target, $fragment),

			(ColumnBuffer::Uint(l), ColumnBuffer::Float4(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Float8(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Int1(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Int2(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Int4(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Int8(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Int16(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Uint1(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Uint2(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Uint4(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Uint8(r)) => $ah($ctx, &uints(l), r.values(), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Uint16(r)) => $ah($ctx, &uints(l), &u128s(r), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Int(r)) => $ah($ctx, &uints(l), &ints(r), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Uint(r)) => $ah($ctx, &uints(l), &uints(r), $target, $fragment),
			(ColumnBuffer::Uint(l), ColumnBuffer::Decimal(r)) => $ah($ctx, &uints(l), &decimals(r), $target, $fragment),

			(ColumnBuffer::Decimal(l), ColumnBuffer::Float4(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Float8(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int1(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int2(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int4(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int8(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int16(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint1(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint2(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint4(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint8(r)) => $ah($ctx, &decimals(l), r.values(), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint16(r)) => $ah($ctx, &decimals(l), &u128s(r), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Int(r)) => $ah($ctx, &decimals(l), &ints(r), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Uint(r)) => $ah($ctx, &decimals(l), &uints(r), $target, $fragment),
			(ColumnBuffer::Decimal(l), ColumnBuffer::Decimal(r)) => $ah($ctx, &decimals(l), &decimals(r), $target, $fragment),


			$($extra)*
		}
	};


	(@values Uint16 $array:ident) => {
		&u128s($array)
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

fn is_unsigned(ty: &ValueType) -> bool {
	matches!(
		ty,
		ValueType::Uint { .. }
			| ValueType::Uint1
			| ValueType::Uint2
			| ValueType::Uint4
			| ValueType::Uint8
			| ValueType::Uint16
	)
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
	let decimal =
		[&left, &right].iter().any(|ty| ty.is_floating_point() || matches!(ty, ValueType::Decimal { .. }));
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
		ArithOp::Div if decimal => (left_digits + right_scale, left_scale.max(right_scale).max(6)),
		ArithOp::Div => (left_digits, 0),
		ArithOp::Rem => (left_digits.min(right_digits), left_scale.max(right_scale)),
	};
	let scale = scale.min(MAX_DIGITS);
	let precision = Precision::new(digits.saturating_add(scale).clamp(1, MAX_DIGITS));
	if decimal {
		ValueType::decimal(precision, Scale::new(scale))
	} else if is_unsigned(&left) && is_unsigned(&right) {
		ValueType::uint(precision)
	} else {
		ValueType::int(precision)
	}
}
