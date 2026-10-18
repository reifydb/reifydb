// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::LazyFragment,
	value::{
		constraint::precision::Precision,
		decimal::{Decimal, unscaled},
		is::IsNumber,
		number::{
			promote::Promote,
			safe::{add::SafeAdd, div::SafeDiv, mul::SafeMul, remainder::SafeRemainder, sub::SafeSub},
		},
		value_type::{ValueType, get::GetType},
	},
};

use crate::{
	interface::catalog::property::ColumnSaturationStrategy,
	value::column::arith::context::{ArithContext, Overflow},
};

enum Mode {
	Error,
	None,
	Saturate,
	Wrap,
}

impl ArithContext {
	fn mode(&self) -> Mode {
		match self.overflow {
			Overflow::Target => match self.saturation_policy() {
				ColumnSaturationStrategy::Error => Mode::Error,
				ColumnSaturationStrategy::None => Mode::None,
			},
			Overflow::Error => Mode::Error,
			Overflow::None => Mode::None,
			Overflow::Saturate => Mode::Saturate,
			Overflow::Wrap => Mode::Wrap,
		}
	}

	fn out_of_range(&self, target: ValueType, fragment: impl LazyFragment) -> Error {
		TypeError::NumberOutOfRange {
			target,
			fragment: fragment.fragment(),
			descriptor: self.target.as_ref().and_then(|c| c.to_number_descriptor()),
		}
		.into()
	}
}

macro_rules! impl_scalar_op {
	($method:ident, $safe_trait:ident, $checked:ident, $saturating:ident, $wrapping:ident $(, $zero:ident)?) => {
		impl ArithContext {
			pub fn $method<L, R>(
				&self,
				l: &L,
				r: &R,
				fragment: impl LazyFragment + Copy,
			) -> Result<Option<<L as Promote<R>>::Output>>
			where
				L: Promote<R>,
				R: IsNumber,
				<L as Promote<R>>::Output: IsNumber,
				<L as Promote<R>>::Output: $safe_trait,
			{
				let target = || <L as Promote<R>>::Output::get_type();
				let mode = self.mode();
				let Some((lp, rp)) = l.checked_promote(r) else {
					return match mode {
						Mode::None => Ok(None),
						_ => Err(self.out_of_range(target(), fragment)),
					};
				};
				match mode {
					Mode::Error => {
						$(
							if <<L as Promote<R>>::Output as $safe_trait>::$zero(&rp) {
								return Err(TypeError::DivisionByZero {
									target: target(),
									fragment: fragment.fragment(),
								}
								.into());
							}
						)?
						lp.$checked(&rp).ok_or_else(|| self.out_of_range(target(), fragment)).map(Some)
					}
					Mode::None => Ok(lp.$checked(&rp)),
					Mode::Saturate => Ok(Some(lp.$saturating(&rp))),
					Mode::Wrap => Ok(Some(lp.$wrapping(&rp))),
				}
			}
		}
	};
}

pub trait FitFamily: Sized {
	fn fit_family(&self, target: &ValueType) -> Option<Self>;

	fn clamp_family(&self, target: &ValueType) -> Self;
}

impl FitFamily for Decimal {
	fn fit_family(&self, target: &ValueType) -> Option<Self> {
		let (Some(precision), Some(scale)) = (target.precision(), target.scale()) else {
			return Some(self.clone());
		};
		let rounded = self.round_to_scale(scale.value())?;
		(rounded.digits() <= precision.value()).then_some(rounded)
	}

	fn clamp_family(&self, target: &ValueType) -> Self {
		let (Some(precision), Some(scale)) = (target.precision(), target.scale()) else {
			return self.clone();
		};
		Decimal::from_parts(family_bound(precision, self.is_negative()), scale.value())
			.expect("a precision bound is in range")
	}
}

fn family_bound(precision: Precision, negative: bool) -> i256 {
	let max = unscaled::pow10(precision.value()).expect("a precision is at most 76 digits").wrapping_sub(i256::ONE);
	if negative {
		max.wrapping_neg()
	} else {
		max
	}
}

impl ArithContext {
	pub fn fit_family<T: FitFamily>(
		&self,
		value: T,
		target: &ValueType,
		fragment: impl LazyFragment + Copy,
	) -> Result<Option<T>> {
		match (value.fit_family(target), self.mode()) {
			(Some(fitted), _) => Ok(Some(fitted)),
			(None, Mode::None) => Ok(None),
			(None, Mode::Error) => Err(self.out_of_range(target.clone(), fragment)),
			(None, Mode::Saturate | Mode::Wrap) => Ok(Some(value.clamp_family(target))),
		}
	}
}

impl_scalar_op!(add, SafeAdd, checked_add, saturating_add, wrapping_add);
impl_scalar_op!(sub, SafeSub, checked_sub, saturating_sub, wrapping_sub);
impl_scalar_op!(mul, SafeMul, checked_mul, saturating_mul, wrapping_mul);
impl_scalar_op!(div, SafeDiv, checked_div, saturating_div, wrapping_div, is_zero);
impl_scalar_op!(remainder, SafeRemainder, checked_rem, saturating_rem, wrapping_rem, is_zero);

#[cfg(test)]
pub mod tests {
	use reifydb_value::fragment::Fragment;

	use crate::value::column::arith::context::ArithContext;

	#[test]
	fn test_add() {
		let test_instance = ArithContext::default();
		let result = test_instance.add(&1i8, &255i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(256i128)));
	}

	#[test]
	fn test_sub() {
		let test_instance = ArithContext::default();
		let result = test_instance.sub(&1i8, &255i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(-254i128)));
	}

	#[test]
	fn test_mul() {
		let test_instance = ArithContext::default();
		let result = test_instance.mul(&23i8, &255i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(5865i128)));
	}

	#[test]
	fn test_div() {
		let test_instance = ArithContext::default();
		let result = test_instance.div(&120i8, &20i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(6i128)));
	}

	#[test]
	fn test_remainder() {
		let test_instance = ArithContext::default();
		let result = test_instance.remainder(&120i8, &21i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(15i128)));
	}
}
