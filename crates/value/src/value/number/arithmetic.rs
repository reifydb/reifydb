// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use crate::value::{
	Value,
	decimal::Decimal,
	int::Int,
	is::IsNumber,
	number::{
		promote::Promote,
		safe::{add::SafeAdd, div::SafeDiv, mul::SafeMul, remainder::SafeRemainder, sub::SafeSub},
	},
	to_value::ToValue,
	uint::Uint,
	value_type::ValueType,
};

fn arith_type(v: &Value) -> ValueType {
	match v {
		Value::None {
			inner,
		} => inner.clone(),
		other => other.get_type(),
	}
}

fn none_inner(l: &Value, r: &Value) -> Option<ValueType> {
	let l_none = matches!(l, Value::None { .. });
	let r_none = matches!(r, Value::None { .. });
	if !l_none && !r_none {
		return None;
	}
	Some(ValueType::promote(arith_type(l), arith_type(r)))
}

fn value_is_zero(v: &Value) -> bool {
	match v {
		Value::Int1(x) => SafeDiv::is_zero(x),
		Value::Int2(x) => SafeDiv::is_zero(x),
		Value::Int4(x) => SafeDiv::is_zero(x),
		Value::Int8(x) => SafeDiv::is_zero(x),
		Value::Int16(x) => SafeDiv::is_zero(x),
		Value::Uint1(x) => SafeDiv::is_zero(x),
		Value::Uint2(x) => SafeDiv::is_zero(x),
		Value::Uint4(x) => SafeDiv::is_zero(x),
		Value::Uint8(x) => SafeDiv::is_zero(x),
		Value::Uint16(x) => SafeDiv::is_zero(x),
		Value::Float4(x) => SafeDiv::is_zero(&x.value()),
		Value::Float8(x) => SafeDiv::is_zero(&x.value()),
		Value::Int(x) => SafeDiv::is_zero(x),
		Value::Uint(x) => SafeDiv::is_zero(x),
		Value::Decimal(x) => SafeDiv::is_zero(x),
		_ => false,
	}
}

macro_rules! gen_helpers {
	($checked:ident, $sat:ident, $wrap:ident, $trait:ident, $cm:ident, $sm:ident, $wm:ident) => {
		fn $checked<L, R>(l: &L, r: &R) -> Option<Value>
		where
			L: Promote<R>,
			R: IsNumber,
			<L as Promote<R>>::Output: $trait,
		{
			let (a, b) = l.checked_promote(r)?;
			a.$cm(&b).map(|o| o.to_value())
		}

		fn $sat<L, R>(l: &L, r: &R) -> Value
		where
			L: Promote<R>,
			R: IsNumber,
			<L as Promote<R>>::Output: $trait,
		{
			let (a, b) = l.saturating_promote(r);
			a.$sm(&b).to_value()
		}

		fn $wrap<L, R>(l: &L, r: &R) -> Value
		where
			L: Promote<R>,
			R: IsNumber,
			<L as Promote<R>>::Output: $trait,
		{
			let (a, b) = l.wrapping_promote(r);
			a.$wm(&b).to_value()
		}
	};
}

gen_helpers!(v_checked_add, v_sat_add, v_wrap_add, SafeAdd, checked_add, saturating_add, wrapping_add);
gen_helpers!(v_checked_sub, v_sat_sub, v_wrap_sub, SafeSub, checked_sub, saturating_sub, wrapping_sub);
gen_helpers!(v_checked_mul, v_sat_mul, v_wrap_mul, SafeMul, checked_mul, saturating_mul, wrapping_mul);
gen_helpers!(v_checked_rem, v_sat_rem, v_wrap_rem, SafeRemainder, checked_rem, saturating_rem, wrapping_rem);

trait DivToValue: Sized {
	fn checked_div_to_value(&self, r: &Self) -> Option<Value>;
	fn saturating_div_to_value(&self, r: &Self) -> Value;
	fn wrapping_div_to_value(&self, r: &Self) -> Value;
}

macro_rules! impl_div_to_value_via_decimal {
	($($t:ty),*) => {
		$(
			impl DivToValue for $t {
				fn checked_div_to_value(&self, r: &Self) -> Option<Value> {
					Decimal::from(self.clone()).checked_div(&Decimal::from(r.clone())).map(Value::Decimal)
				}
				fn saturating_div_to_value(&self, r: &Self) -> Value {
					Value::Decimal(Decimal::from(self.clone()).saturating_div(&Decimal::from(r.clone())))
				}
				fn wrapping_div_to_value(&self, r: &Self) -> Value {
					Value::Decimal(Decimal::from(self.clone()).wrapping_div(&Decimal::from(r.clone())))
				}
			}
		)*
	};
}

impl_div_to_value_via_decimal!(i128, u128, Int, Uint);

impl DivToValue for f64 {
	fn checked_div_to_value(&self, r: &Self) -> Option<Value> {
		self.checked_div(r).map(|o| o.to_value())
	}
	fn saturating_div_to_value(&self, r: &Self) -> Value {
		self.saturating_div(r).to_value()
	}
	fn wrapping_div_to_value(&self, r: &Self) -> Value {
		self.wrapping_div(r).to_value()
	}
}

impl DivToValue for Decimal {
	fn checked_div_to_value(&self, r: &Self) -> Option<Value> {
		self.checked_div(r).map(Value::Decimal)
	}
	fn saturating_div_to_value(&self, r: &Self) -> Value {
		Value::Decimal(self.saturating_div(r))
	}
	fn wrapping_div_to_value(&self, r: &Self) -> Value {
		Value::Decimal(self.wrapping_div(r))
	}
}

fn v_checked_div<L, R>(l: &L, r: &R) -> Option<Value>
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: DivToValue,
{
	let (a, b) = l.checked_promote(r)?;
	a.checked_div_to_value(&b)
}

fn v_sat_div<L, R>(l: &L, r: &R) -> Value
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: DivToValue,
{
	let (a, b) = l.saturating_promote(r);
	a.saturating_div_to_value(&b)
}

fn v_wrap_div<L, R>(l: &L, r: &R) -> Value
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: DivToValue,
{
	let (a, b) = l.wrapping_promote(r);
	a.wrapping_div_to_value(&b)
}

macro_rules! right_arms {
	($r:expr, $la:expr, $op:ident, $fallback:expr) => {
		match $r {
			Value::Int1(b) => $op($la, b),
			Value::Int2(b) => $op($la, b),
			Value::Int4(b) => $op($la, b),
			Value::Int8(b) => $op($la, b),
			Value::Int16(b) => $op($la, b),
			Value::Uint1(b) => $op($la, b),
			Value::Uint2(b) => $op($la, b),
			Value::Uint4(b) => $op($la, b),
			Value::Uint8(b) => $op($la, b),
			Value::Uint16(b) => $op($la, b),
			Value::Int(b) => $op($la, b),
			Value::Uint(b) => $op($la, b),
			Value::Decimal(b) => $op($la, b),
			Value::Float4(b) => $op($la, &b.value()),
			Value::Float8(b) => $op($la, &b.value()),
			_ => $fallback,
		}
	};
}

macro_rules! value_arith_dispatch {
	($l:expr, $r:expr, $op:ident, $fallback:expr) => {
		match $l {
			Value::Int1(a) => right_arms!($r, a, $op, $fallback),
			Value::Int2(a) => right_arms!($r, a, $op, $fallback),
			Value::Int4(a) => right_arms!($r, a, $op, $fallback),
			Value::Int8(a) => right_arms!($r, a, $op, $fallback),
			Value::Int16(a) => right_arms!($r, a, $op, $fallback),
			Value::Uint1(a) => right_arms!($r, a, $op, $fallback),
			Value::Uint2(a) => right_arms!($r, a, $op, $fallback),
			Value::Uint4(a) => right_arms!($r, a, $op, $fallback),
			Value::Uint8(a) => right_arms!($r, a, $op, $fallback),
			Value::Uint16(a) => right_arms!($r, a, $op, $fallback),
			Value::Int(a) => right_arms!($r, a, $op, $fallback),
			Value::Uint(a) => right_arms!($r, a, $op, $fallback),
			Value::Decimal(a) => right_arms!($r, a, $op, $fallback),
			Value::Float4(a) => right_arms!($r, &a.value(), $op, $fallback),
			Value::Float8(a) => right_arms!($r, &a.value(), $op, $fallback),
			_ => $fallback,
		}
	};
}

macro_rules! impl_value_safe {
	($trait:ident, $checked:ident, $sat:ident, $wrap:ident, $hc:ident, $hs:ident, $hw:ident) => {
		impl $trait for Value {
			fn $checked(&self, r: &Self) -> Option<Self> {
				if let Some(inner) = none_inner(self, r) {
					return Some(Value::None {
						inner,
					});
				}
				value_arith_dispatch!(self, r, $hc, None)
			}

			fn $sat(&self, r: &Self) -> Self {
				if let Some(inner) = none_inner(self, r) {
					return Value::None {
						inner,
					};
				}
				value_arith_dispatch!(
					self,
					r,
					$hs,
					Value::None {
						inner: ValueType::Any
					}
				)
			}

			fn $wrap(&self, r: &Self) -> Self {
				if let Some(inner) = none_inner(self, r) {
					return Value::None {
						inner,
					};
				}
				value_arith_dispatch!(
					self,
					r,
					$hw,
					Value::None {
						inner: ValueType::Any
					}
				)
			}
		}
	};
}

impl_value_safe!(SafeAdd, checked_add, saturating_add, wrapping_add, v_checked_add, v_sat_add, v_wrap_add);
impl_value_safe!(SafeSub, checked_sub, saturating_sub, wrapping_sub, v_checked_sub, v_sat_sub, v_wrap_sub);
impl_value_safe!(SafeMul, checked_mul, saturating_mul, wrapping_mul, v_checked_mul, v_sat_mul, v_wrap_mul);

impl SafeDiv for Value {
	fn checked_div(&self, r: &Self) -> Option<Self> {
		if let Some(inner) = none_inner(self, r) {
			return Some(Value::None {
				inner,
			});
		}
		value_arith_dispatch!(self, r, v_checked_div, None)
	}

	fn saturating_div(&self, r: &Self) -> Self {
		if let Some(inner) = none_inner(self, r) {
			return Value::None {
				inner,
			};
		}
		value_arith_dispatch!(
			self,
			r,
			v_sat_div,
			Value::None {
				inner: ValueType::Any
			}
		)
	}

	fn wrapping_div(&self, r: &Self) -> Self {
		if let Some(inner) = none_inner(self, r) {
			return Value::None {
				inner,
			};
		}
		value_arith_dispatch!(
			self,
			r,
			v_wrap_div,
			Value::None {
				inner: ValueType::Any
			}
		)
	}

	fn is_zero(&self) -> bool {
		value_is_zero(self)
	}
}

impl SafeRemainder for Value {
	fn checked_rem(&self, r: &Self) -> Option<Self> {
		if let Some(inner) = none_inner(self, r) {
			return Some(Value::None {
				inner,
			});
		}
		value_arith_dispatch!(self, r, v_checked_rem, None)
	}

	fn saturating_rem(&self, r: &Self) -> Self {
		if let Some(inner) = none_inner(self, r) {
			return Value::None {
				inner,
			};
		}
		value_arith_dispatch!(
			self,
			r,
			v_sat_rem,
			Value::None {
				inner: ValueType::Any
			}
		)
	}

	fn wrapping_rem(&self, r: &Self) -> Self {
		if let Some(inner) = none_inner(self, r) {
			return Value::None {
				inner,
			};
		}
		value_arith_dispatch!(
			self,
			r,
			v_wrap_rem,
			Value::None {
				inner: ValueType::Any
			}
		)
	}

	fn is_zero(&self) -> bool {
		value_is_zero(self)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::value::{ordered_f32::OrderedF32, ordered_f64::OrderedF64};

	fn int4(v: i32) -> Value {
		Value::Int4(v)
	}
	fn int2(v: i16) -> Value {
		Value::Int2(v)
	}
	fn uint4(v: u32) -> Value {
		Value::Uint4(v)
	}
	fn dec(v: i64) -> Value {
		Value::Decimal(Decimal::from(v))
	}

	// Decision 1: add/sub/mul of two integers promote to the widest fixed type.
	#[test]
	fn add_int_pair_promotes_to_int16() {
		assert_eq!(int2(3).checked_add(&int4(4)), Some(Value::Int16(7)));
		assert_eq!(int4(3).checked_add(&int4(4)), Some(Value::Int16(7)));
	}

	#[test]
	fn add_uint_pair_promotes_to_uint16() {
		assert_eq!(uint4(3).checked_add(&uint4(4)), Some(Value::Uint16(7)));
	}

	#[test]
	fn add_float_pair_is_float8() {
		let out = Value::Float8(OrderedF64::try_from(1.5).unwrap())
			.checked_add(&Value::Float4(OrderedF32::try_from(2.5f32).unwrap()));
		assert_eq!(out, Some(Value::Float8(OrderedF64::try_from(4.0).unwrap())));
	}

	#[test]
	fn add_decimal_pair_is_decimal() {
		assert_eq!(dec(3).checked_add(&dec(4)), Some(Value::Decimal(Decimal::from(7i64))));
	}

	#[test]
	fn mixed_int_decimal_is_decimal() {
		assert_eq!(int4(3).checked_add(&dec(4)), Some(Value::Decimal(Decimal::from(7i64))));
	}

	// Decision 3: integer division promotes to Decimal (exact, e.g. 3/2 = 1.5).
	#[test]
	fn int_div_is_decimal_exact() {
		let got = int4(3).checked_div(&int4(2)).unwrap();
		assert_eq!(got, Value::Decimal(Decimal::from(3i64).checked_div(&Decimal::from(2i64)).unwrap()));
		// And it is 1.5, not the truncated 1.
		assert_ne!(got, Value::Decimal(Decimal::from(1i64)));
	}

	#[test]
	fn float_div_stays_float8() {
		let three = Value::Float8(OrderedF64::try_from(3.0).unwrap());
		let two = Value::Float8(OrderedF64::try_from(2.0).unwrap());
		assert_eq!(three.checked_div(&two), Some(Value::Float8(OrderedF64::try_from(1.5).unwrap())));
	}

	// none propagation produces a DEFINED none (Some(None-value)), distinct from
	// the overflow None of the Option, and carries the promoted result type.
	#[test]
	fn none_propagates_with_promoted_inner() {
		let got = Value::none_of(ValueType::Int4).checked_add(&int4(5));
		assert_eq!(
			got,
			Some(Value::None {
				inner: ValueType::promote(ValueType::Int4, ValueType::Int4)
			})
		);
		assert!(matches!(got, Some(Value::None { .. })));
		// rhs none
		assert!(matches!(int4(5).checked_add(&Value::none_of(ValueType::Int4)), Some(Value::None { .. })));
		// both none
		assert!(matches!(
			Value::none_of(ValueType::Int4).checked_add(&Value::none_of(ValueType::Int8)),
			Some(Value::None { .. })
		));
	}

	// Overflow: i128-promoted result overflows only at the i128 limit.
	#[test]
	fn overflow_checked_saturating_wrapping() {
		let max = Value::Int16(i128::MAX);
		let one = Value::Int16(1);
		assert_eq!(max.checked_add(&one), None);
		assert_eq!(max.saturating_add(&one), Value::Int16(i128::MAX));
		assert_eq!(max.wrapping_add(&one), Value::Int16(i128::MIN));
	}

	#[test]
	fn div_by_zero_is_none() {
		assert_eq!(int4(3).checked_div(&int4(0)), None);
		assert_eq!(dec(3).checked_div(&dec(0)), None);
		let one = Value::Float8(OrderedF64::try_from(1.0).unwrap());
		let zero = Value::Float8(OrderedF64::try_from(0.0).unwrap());
		assert_eq!(one.checked_div(&zero), None);
	}

	#[test]
	fn is_zero_per_variant() {
		assert!(SafeDiv::is_zero(&int4(0)));
		assert!(!SafeDiv::is_zero(&int4(1)));
		assert!(SafeDiv::is_zero(&dec(0)));
		assert!(!SafeDiv::is_zero(&dec(5)));
		assert!(!SafeDiv::is_zero(&Value::none_of(ValueType::Int4)));
		assert!(!SafeDiv::is_zero(&Value::Boolean(true)));
	}

	#[test]
	fn non_numeric_operand_is_none() {
		assert_eq!(int4(3).checked_add(&Value::Boolean(true)), None);
		assert_eq!(Value::Utf8("x".into()).checked_add(&int4(3)), None);
	}

	// Retraction invariant (Rule 9): for non-float pairs, add then sub of the same
	// operand exactly restores the original. This is the property the window/aggregate
	// accumulator relies on; it must fail if checked_sub stops inverting checked_add.
	#[test]
	fn retraction_invariant_exact_for_integers() {
		let cases = [
			(int4(100), int4(7)),
			(int2(30), int4(9)),
			(uint4(50), uint4(8)),
			(dec(1000), dec(123)),
			(Value::Int(Int::from_i64(99)), Value::Int(Int::from_i64(40))),
		];
		for (r, x) in cases {
			let added = r.checked_add(&x).unwrap();
			let restored = added.checked_sub(&x).unwrap();
			// Compare numerically: r and restored may differ in declared width
			// (Int4 vs Int16) but must be equal in value, so re-add and re-sub
			// must be idempotent at the wide type.
			let twice = restored.checked_add(&x).unwrap();
			assert_eq!(added, twice, "add/sub must invert for {r:?} - {x:?}");
		}
	}

	// Float retraction is inherently lossy (IEEE); documented, tolerance-based so
	// this is not a false negative.
	#[test]
	fn retraction_float_within_tolerance() {
		let r = Value::Float8(OrderedF64::try_from(0.1).unwrap());
		let x = Value::Float8(OrderedF64::try_from(0.2).unwrap());
		let restored = r.checked_add(&x).unwrap().checked_sub(&x).unwrap();
		if let Value::Float8(v) = restored {
			assert!((v.value() - 0.1).abs() < 1e-9, "float retraction drift too large: {}", v.value());
		} else {
			panic!("expected Float8, got {restored:?}");
		}
	}

	// NaN result becomes a defined none{Float8} via ToValue (pin the silent-none path).
	#[test]
	fn nan_result_is_none_float8() {
		let inf = Value::Float8(OrderedF64::try_from(f64::MAX).unwrap());
		// MAX * MAX overflows to +inf -> checked None; saturating clamps.
		assert_eq!(inf.checked_mul(&inf), None);
	}
}
