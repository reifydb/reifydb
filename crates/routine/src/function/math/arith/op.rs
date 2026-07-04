// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	is::IsNumber,
	number::safe::{add::SafeAdd, div::SafeDiv, mul::SafeMul, remainder::SafeRemainder, sub::SafeSub},
};

pub trait SafeNum: SafeAdd + SafeSub + SafeMul + SafeDiv + SafeRemainder + IsNumber + Default + Clone {}

impl<T: SafeAdd + SafeSub + SafeMul + SafeDiv + SafeRemainder + IsNumber + Default + Clone> SafeNum for T {}

pub trait ArithOp {
	const NAME: &'static str;
	const DIVISIVE: bool = false;

	fn checked<T: SafeNum>(l: &T, r: &T) -> Option<T>;
	fn saturating<T: SafeNum>(l: &T, r: &T) -> T;
	fn wrapping<T: SafeNum>(l: &T, r: &T) -> T;
}

pub struct Add;

impl ArithOp for Add {
	const NAME: &'static str = "add";

	fn checked<T: SafeNum>(l: &T, r: &T) -> Option<T> {
		SafeAdd::checked_add(l, r)
	}
	fn saturating<T: SafeNum>(l: &T, r: &T) -> T {
		SafeAdd::saturating_add(l, r)
	}
	fn wrapping<T: SafeNum>(l: &T, r: &T) -> T {
		SafeAdd::wrapping_add(l, r)
	}
}

pub struct Sub;

impl ArithOp for Sub {
	const NAME: &'static str = "sub";

	fn checked<T: SafeNum>(l: &T, r: &T) -> Option<T> {
		SafeSub::checked_sub(l, r)
	}
	fn saturating<T: SafeNum>(l: &T, r: &T) -> T {
		SafeSub::saturating_sub(l, r)
	}
	fn wrapping<T: SafeNum>(l: &T, r: &T) -> T {
		SafeSub::wrapping_sub(l, r)
	}
}

pub struct Mul;

impl ArithOp for Mul {
	const NAME: &'static str = "mul";

	fn checked<T: SafeNum>(l: &T, r: &T) -> Option<T> {
		SafeMul::checked_mul(l, r)
	}
	fn saturating<T: SafeNum>(l: &T, r: &T) -> T {
		SafeMul::saturating_mul(l, r)
	}
	fn wrapping<T: SafeNum>(l: &T, r: &T) -> T {
		SafeMul::wrapping_mul(l, r)
	}
}

pub struct Div;

impl ArithOp for Div {
	const NAME: &'static str = "div";
	const DIVISIVE: bool = true;

	fn checked<T: SafeNum>(l: &T, r: &T) -> Option<T> {
		SafeDiv::checked_div(l, r)
	}
	fn saturating<T: SafeNum>(l: &T, r: &T) -> T {
		SafeDiv::saturating_div(l, r)
	}
	fn wrapping<T: SafeNum>(l: &T, r: &T) -> T {
		SafeDiv::wrapping_div(l, r)
	}
}

pub struct Rem;

impl ArithOp for Rem {
	const NAME: &'static str = "rem";
	const DIVISIVE: bool = true;

	fn checked<T: SafeNum>(l: &T, r: &T) -> Option<T> {
		SafeRemainder::checked_rem(l, r)
	}
	fn saturating<T: SafeNum>(l: &T, r: &T) -> T {
		SafeRemainder::saturating_rem(l, r)
	}
	fn wrapping<T: SafeNum>(l: &T, r: &T) -> T {
		SafeRemainder::wrapping_rem(l, r)
	}
}
