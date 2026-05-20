// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use num_traits::Zero;
use reifydb_type::value::{decimal::Decimal, int::Int, uint::Uint};

pub trait BinaryOp {
	const NAME: &'static str;

	fn checked_i8(a: i8, b: i8) -> Option<i8>;
	fn checked_i16(a: i16, b: i16) -> Option<i16>;
	fn checked_i32(a: i32, b: i32) -> Option<i32>;
	fn checked_i64(a: i64, b: i64) -> Option<i64>;
	fn checked_i128(a: i128, b: i128) -> Option<i128>;
	fn checked_u8(a: u8, b: u8) -> Option<u8>;
	fn checked_u16(a: u16, b: u16) -> Option<u16>;
	fn checked_u32(a: u32, b: u32) -> Option<u32>;
	fn checked_u64(a: u64, b: u64) -> Option<u64>;
	fn checked_u128(a: u128, b: u128) -> Option<u128>;

	fn saturating_i8(a: i8, b: i8) -> i8;
	fn saturating_i16(a: i16, b: i16) -> i16;
	fn saturating_i32(a: i32, b: i32) -> i32;
	fn saturating_i64(a: i64, b: i64) -> i64;
	fn saturating_i128(a: i128, b: i128) -> i128;
	fn saturating_u8(a: u8, b: u8) -> u8;
	fn saturating_u16(a: u16, b: u16) -> u16;
	fn saturating_u32(a: u32, b: u32) -> u32;
	fn saturating_u64(a: u64, b: u64) -> u64;
	fn saturating_u128(a: u128, b: u128) -> u128;

	fn wrapping_i8(a: i8, b: i8) -> i8;
	fn wrapping_i16(a: i16, b: i16) -> i16;
	fn wrapping_i32(a: i32, b: i32) -> i32;
	fn wrapping_i64(a: i64, b: i64) -> i64;
	fn wrapping_i128(a: i128, b: i128) -> i128;
	fn wrapping_u8(a: u8, b: u8) -> u8;
	fn wrapping_u16(a: u16, b: u16) -> u16;
	fn wrapping_u32(a: u32, b: u32) -> u32;
	fn wrapping_u64(a: u64, b: u64) -> u64;
	fn wrapping_u128(a: u128, b: u128) -> u128;

	fn f32_eval(a: f32, b: f32) -> f32;
	fn f64_eval(a: f64, b: f64) -> f64;

	fn int_eval(a: &Int, b: &Int) -> Int;
	fn uint_eval(a: &Uint, b: &Uint) -> Uint;
	fn decimal_eval(a: &Decimal, b: &Decimal) -> Decimal;

	fn int_eval_checked(a: &Int, b: &Int) -> Option<Int> {
		Some(Self::int_eval(a, b))
	}
	fn uint_eval_checked(a: &Uint, b: &Uint) -> Option<Uint> {
		Some(Self::uint_eval(a, b))
	}
	fn decimal_eval_checked(a: &Decimal, b: &Decimal) -> Option<Decimal> {
		Some(Self::decimal_eval(a, b))
	}
}

macro_rules! impl_binary_op {
	(
		$struct:ident,
		$name:expr,
		$checked:ident,
		$saturating:ident,
		$wrapping:ident,
		$primitive_op:tt
	) => {
		pub struct $struct;

		impl BinaryOp for $struct {
			const NAME: &'static str = $name;

			fn checked_i8(a: i8, b: i8) -> Option<i8> { a.$checked(b) }
			fn checked_i16(a: i16, b: i16) -> Option<i16> { a.$checked(b) }
			fn checked_i32(a: i32, b: i32) -> Option<i32> { a.$checked(b) }
			fn checked_i64(a: i64, b: i64) -> Option<i64> { a.$checked(b) }
			fn checked_i128(a: i128, b: i128) -> Option<i128> { a.$checked(b) }
			fn checked_u8(a: u8, b: u8) -> Option<u8> { a.$checked(b) }
			fn checked_u16(a: u16, b: u16) -> Option<u16> { a.$checked(b) }
			fn checked_u32(a: u32, b: u32) -> Option<u32> { a.$checked(b) }
			fn checked_u64(a: u64, b: u64) -> Option<u64> { a.$checked(b) }
			fn checked_u128(a: u128, b: u128) -> Option<u128> { a.$checked(b) }

			fn saturating_i8(a: i8, b: i8) -> i8 { a.$saturating(b) }
			fn saturating_i16(a: i16, b: i16) -> i16 { a.$saturating(b) }
			fn saturating_i32(a: i32, b: i32) -> i32 { a.$saturating(b) }
			fn saturating_i64(a: i64, b: i64) -> i64 { a.$saturating(b) }
			fn saturating_i128(a: i128, b: i128) -> i128 { a.$saturating(b) }
			fn saturating_u8(a: u8, b: u8) -> u8 { a.$saturating(b) }
			fn saturating_u16(a: u16, b: u16) -> u16 { a.$saturating(b) }
			fn saturating_u32(a: u32, b: u32) -> u32 { a.$saturating(b) }
			fn saturating_u64(a: u64, b: u64) -> u64 { a.$saturating(b) }
			fn saturating_u128(a: u128, b: u128) -> u128 { a.$saturating(b) }

			fn wrapping_i8(a: i8, b: i8) -> i8 { a.$wrapping(b) }
			fn wrapping_i16(a: i16, b: i16) -> i16 { a.$wrapping(b) }
			fn wrapping_i32(a: i32, b: i32) -> i32 { a.$wrapping(b) }
			fn wrapping_i64(a: i64, b: i64) -> i64 { a.$wrapping(b) }
			fn wrapping_i128(a: i128, b: i128) -> i128 { a.$wrapping(b) }
			fn wrapping_u8(a: u8, b: u8) -> u8 { a.$wrapping(b) }
			fn wrapping_u16(a: u16, b: u16) -> u16 { a.$wrapping(b) }
			fn wrapping_u32(a: u32, b: u32) -> u32 { a.$wrapping(b) }
			fn wrapping_u64(a: u64, b: u64) -> u64 { a.$wrapping(b) }
			fn wrapping_u128(a: u128, b: u128) -> u128 { a.$wrapping(b) }

			fn f32_eval(a: f32, b: f32) -> f32 { a $primitive_op b }
			fn f64_eval(a: f64, b: f64) -> f64 { a $primitive_op b }

			fn int_eval(a: &Int, b: &Int) -> Int { Int(&a.0 $primitive_op &b.0) }
			fn uint_eval(a: &Uint, b: &Uint) -> Uint { Uint(&a.0 $primitive_op &b.0) }
			fn decimal_eval(a: &Decimal, b: &Decimal) -> Decimal { a.clone() $primitive_op b.clone() }
		}
	};
}

impl_binary_op!(Sub, "sub", checked_sub, saturating_sub, wrapping_sub, -);
impl_binary_op!(Add, "add", checked_add, saturating_add, wrapping_add, +);
impl_binary_op!(Mul, "mul", checked_mul, saturating_mul, wrapping_mul, *);

pub struct Div;

macro_rules! div_signed_safe {
	($a:expr, $b:expr, $t:ty) => {{
		let a = $a;
		let b = $b;
		if b == 0 {
			if a >= 0 {
				<$t>::MAX
			} else {
				<$t>::MIN
			}
		} else if a == <$t>::MIN && b == -1 {
			<$t>::MAX
		} else {
			a / b
		}
	}};
}

macro_rules! div_unsigned_safe {
	($a:expr, $b:expr, $t:ty) => {{
		let a = $a;
		let b = $b;
		if b == 0 {
			<$t>::MAX
		} else {
			a / b
		}
	}};
}

macro_rules! div_wrapping_signed {
	($a:expr, $b:expr, $t:ty) => {{
		let a = $a;
		let b = $b;
		if b == 0 {
			0
		} else if a == <$t>::MIN && b == -1 {
			<$t>::MIN
		} else {
			a.wrapping_div(b)
		}
	}};
}

macro_rules! div_wrapping_unsigned {
	($a:expr, $b:expr, $t:ty) => {{
		let a = $a;
		let b = $b;
		if b == 0 {
			0
		} else {
			a / b
		}
	}};
}

impl BinaryOp for Div {
	const NAME: &'static str = "div";

	fn checked_i8(a: i8, b: i8) -> Option<i8> {
		a.checked_div(b)
	}
	fn checked_i16(a: i16, b: i16) -> Option<i16> {
		a.checked_div(b)
	}
	fn checked_i32(a: i32, b: i32) -> Option<i32> {
		a.checked_div(b)
	}
	fn checked_i64(a: i64, b: i64) -> Option<i64> {
		a.checked_div(b)
	}
	fn checked_i128(a: i128, b: i128) -> Option<i128> {
		a.checked_div(b)
	}
	fn checked_u8(a: u8, b: u8) -> Option<u8> {
		a.checked_div(b)
	}
	fn checked_u16(a: u16, b: u16) -> Option<u16> {
		a.checked_div(b)
	}
	fn checked_u32(a: u32, b: u32) -> Option<u32> {
		a.checked_div(b)
	}
	fn checked_u64(a: u64, b: u64) -> Option<u64> {
		a.checked_div(b)
	}
	fn checked_u128(a: u128, b: u128) -> Option<u128> {
		a.checked_div(b)
	}

	fn saturating_i8(a: i8, b: i8) -> i8 {
		div_signed_safe!(a, b, i8)
	}
	fn saturating_i16(a: i16, b: i16) -> i16 {
		div_signed_safe!(a, b, i16)
	}
	fn saturating_i32(a: i32, b: i32) -> i32 {
		div_signed_safe!(a, b, i32)
	}
	fn saturating_i64(a: i64, b: i64) -> i64 {
		div_signed_safe!(a, b, i64)
	}
	fn saturating_i128(a: i128, b: i128) -> i128 {
		div_signed_safe!(a, b, i128)
	}
	fn saturating_u8(a: u8, b: u8) -> u8 {
		div_unsigned_safe!(a, b, u8)
	}
	fn saturating_u16(a: u16, b: u16) -> u16 {
		div_unsigned_safe!(a, b, u16)
	}
	fn saturating_u32(a: u32, b: u32) -> u32 {
		div_unsigned_safe!(a, b, u32)
	}
	fn saturating_u64(a: u64, b: u64) -> u64 {
		div_unsigned_safe!(a, b, u64)
	}
	fn saturating_u128(a: u128, b: u128) -> u128 {
		div_unsigned_safe!(a, b, u128)
	}

	fn wrapping_i8(a: i8, b: i8) -> i8 {
		div_wrapping_signed!(a, b, i8)
	}
	fn wrapping_i16(a: i16, b: i16) -> i16 {
		div_wrapping_signed!(a, b, i16)
	}
	fn wrapping_i32(a: i32, b: i32) -> i32 {
		div_wrapping_signed!(a, b, i32)
	}
	fn wrapping_i64(a: i64, b: i64) -> i64 {
		div_wrapping_signed!(a, b, i64)
	}
	fn wrapping_i128(a: i128, b: i128) -> i128 {
		div_wrapping_signed!(a, b, i128)
	}
	fn wrapping_u8(a: u8, b: u8) -> u8 {
		div_wrapping_unsigned!(a, b, u8)
	}
	fn wrapping_u16(a: u16, b: u16) -> u16 {
		div_wrapping_unsigned!(a, b, u16)
	}
	fn wrapping_u32(a: u32, b: u32) -> u32 {
		div_wrapping_unsigned!(a, b, u32)
	}
	fn wrapping_u64(a: u64, b: u64) -> u64 {
		div_wrapping_unsigned!(a, b, u64)
	}
	fn wrapping_u128(a: u128, b: u128) -> u128 {
		div_wrapping_unsigned!(a, b, u128)
	}

	fn f32_eval(a: f32, b: f32) -> f32 {
		a / b
	}
	fn f64_eval(a: f64, b: f64) -> f64 {
		a / b
	}

	fn int_eval(a: &Int, b: &Int) -> Int {
		if b.0.is_zero() {
			Int::zero()
		} else {
			Int(&a.0 / &b.0)
		}
	}
	fn uint_eval(a: &Uint, b: &Uint) -> Uint {
		if b.0.is_zero() {
			Uint::zero()
		} else {
			Uint(&a.0 / &b.0)
		}
	}
	fn decimal_eval(a: &Decimal, b: &Decimal) -> Decimal {
		if b.inner().is_zero() {
			Decimal::default()
		} else {
			a.clone() / b.clone()
		}
	}

	fn int_eval_checked(a: &Int, b: &Int) -> Option<Int> {
		if b.0.is_zero() {
			None
		} else {
			Some(Int(&a.0 / &b.0))
		}
	}
	fn uint_eval_checked(a: &Uint, b: &Uint) -> Option<Uint> {
		if b.0.is_zero() {
			None
		} else {
			Some(Uint(&a.0 / &b.0))
		}
	}
	fn decimal_eval_checked(a: &Decimal, b: &Decimal) -> Option<Decimal> {
		if b.inner().is_zero() {
			None
		} else {
			Some(a.clone() / b.clone())
		}
	}
}

pub struct Rem;

macro_rules! rem_checked_to_zero {
	($a:expr, $b:expr, $checked:ident) => {{ $a.$checked($b).unwrap_or(0) }};
}

macro_rules! rem_wrapping_safe_signed {
	($a:expr, $b:expr, $t:ty) => {{
		let a = $a;
		let b = $b;
		if b == 0 {
			0
		} else if a == <$t>::MIN && b == -1 {
			0
		} else {
			a.wrapping_rem(b)
		}
	}};
}

macro_rules! rem_wrapping_safe_unsigned {
	($a:expr, $b:expr) => {{
		let a = $a;
		let b = $b;
		if b == 0 {
			0
		} else {
			a % b
		}
	}};
}

impl BinaryOp for Rem {
	const NAME: &'static str = "rem";

	fn checked_i8(a: i8, b: i8) -> Option<i8> {
		a.checked_rem(b)
	}
	fn checked_i16(a: i16, b: i16) -> Option<i16> {
		a.checked_rem(b)
	}
	fn checked_i32(a: i32, b: i32) -> Option<i32> {
		a.checked_rem(b)
	}
	fn checked_i64(a: i64, b: i64) -> Option<i64> {
		a.checked_rem(b)
	}
	fn checked_i128(a: i128, b: i128) -> Option<i128> {
		a.checked_rem(b)
	}
	fn checked_u8(a: u8, b: u8) -> Option<u8> {
		a.checked_rem(b)
	}
	fn checked_u16(a: u16, b: u16) -> Option<u16> {
		a.checked_rem(b)
	}
	fn checked_u32(a: u32, b: u32) -> Option<u32> {
		a.checked_rem(b)
	}
	fn checked_u64(a: u64, b: u64) -> Option<u64> {
		a.checked_rem(b)
	}
	fn checked_u128(a: u128, b: u128) -> Option<u128> {
		a.checked_rem(b)
	}

	fn saturating_i8(a: i8, b: i8) -> i8 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_i16(a: i16, b: i16) -> i16 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_i32(a: i32, b: i32) -> i32 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_i64(a: i64, b: i64) -> i64 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_i128(a: i128, b: i128) -> i128 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_u8(a: u8, b: u8) -> u8 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_u16(a: u16, b: u16) -> u16 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_u32(a: u32, b: u32) -> u32 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_u64(a: u64, b: u64) -> u64 {
		rem_checked_to_zero!(a, b, checked_rem)
	}
	fn saturating_u128(a: u128, b: u128) -> u128 {
		rem_checked_to_zero!(a, b, checked_rem)
	}

	fn wrapping_i8(a: i8, b: i8) -> i8 {
		rem_wrapping_safe_signed!(a, b, i8)
	}
	fn wrapping_i16(a: i16, b: i16) -> i16 {
		rem_wrapping_safe_signed!(a, b, i16)
	}
	fn wrapping_i32(a: i32, b: i32) -> i32 {
		rem_wrapping_safe_signed!(a, b, i32)
	}
	fn wrapping_i64(a: i64, b: i64) -> i64 {
		rem_wrapping_safe_signed!(a, b, i64)
	}
	fn wrapping_i128(a: i128, b: i128) -> i128 {
		rem_wrapping_safe_signed!(a, b, i128)
	}
	fn wrapping_u8(a: u8, b: u8) -> u8 {
		rem_wrapping_safe_unsigned!(a, b)
	}
	fn wrapping_u16(a: u16, b: u16) -> u16 {
		rem_wrapping_safe_unsigned!(a, b)
	}
	fn wrapping_u32(a: u32, b: u32) -> u32 {
		rem_wrapping_safe_unsigned!(a, b)
	}
	fn wrapping_u64(a: u64, b: u64) -> u64 {
		rem_wrapping_safe_unsigned!(a, b)
	}
	fn wrapping_u128(a: u128, b: u128) -> u128 {
		rem_wrapping_safe_unsigned!(a, b)
	}

	fn f32_eval(a: f32, b: f32) -> f32 {
		a % b
	}
	fn f64_eval(a: f64, b: f64) -> f64 {
		a % b
	}

	fn int_eval(a: &Int, b: &Int) -> Int {
		if b.0.is_zero() {
			Int::zero()
		} else {
			Int(&a.0 % &b.0)
		}
	}
	fn uint_eval(a: &Uint, b: &Uint) -> Uint {
		if b.0.is_zero() {
			Uint::zero()
		} else {
			Uint(&a.0 % &b.0)
		}
	}
	fn decimal_eval(_a: &Decimal, _b: &Decimal) -> Decimal {
		Decimal::default()
	}

	fn int_eval_checked(a: &Int, b: &Int) -> Option<Int> {
		if b.0.is_zero() {
			None
		} else {
			Some(Int(&a.0 % &b.0))
		}
	}
	fn uint_eval_checked(a: &Uint, b: &Uint) -> Option<Uint> {
		if b.0.is_zero() {
			None
		} else {
			Some(Uint(&a.0 % &b.0))
		}
	}
	fn decimal_eval_checked(_a: &Decimal, b: &Decimal) -> Option<Decimal> {
		if b.inner().is_zero() {
			None
		} else {
			Some(Decimal::default())
		}
	}
}
