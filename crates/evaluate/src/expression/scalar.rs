// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::property::ColumnSaturationStrategy;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::LazyFragment,
	value::{
		decimal::Decimal,
		is::IsNumber,
		number::{
			promote::Promote,
			safe::{add::SafeAdd, div::SafeDiv, mul::SafeMul, remainder::SafeRemainder, sub::SafeSub},
		},
		value_type::{ValueType, get::GetType},
	},
};

use crate::expression::context::EvalContext;

macro_rules! impl_scalar_op {
	($method:ident, $safe_trait:ident, $checked_method:ident) => {
		impl EvalContext<'_> {
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
				match &self.saturation_policy() {
					ColumnSaturationStrategy::Error => {
						let Some((lp, rp)) = l.checked_promote(r) else {
							let descriptor = self
								.target
								.as_ref()
								.and_then(|c| c.to_number_descriptor());
							return Err(TypeError::NumberOutOfRange {
								target: <L as Promote<R>>::Output::get_type(),
								fragment: fragment.fragment(),
								descriptor,
							}
							.into());
						};

						lp.$checked_method(&rp)
							.ok_or_else(|| {
								let descriptor = self
									.target
									.as_ref()
									.and_then(|c| c.to_number_descriptor());
								TypeError::NumberOutOfRange {
									target: <L as Promote<R>>::Output::get_type(),
									fragment: fragment.fragment(),
									descriptor,
								}
								.into()
							})
							.map(Some)
					}
					ColumnSaturationStrategy::None => {
						let Some((lp, rp)) = l.checked_promote(r) else {
							return Ok(None);
						};

						match lp.$checked_method(&rp) {
							None => Ok(None),
							Some(value) => Ok(Some(value)),
						}
					}
				}
			}
		}
	};
}

macro_rules! impl_scalar_divisive_op {
	($method:ident, $safe_trait:ident, $checked_method:ident) => {
		impl EvalContext<'_> {
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
				match &self.saturation_policy() {
					ColumnSaturationStrategy::Error => {
						let Some((lp, rp)) = l.checked_promote(r) else {
							let descriptor = self
								.target
								.as_ref()
								.and_then(|c| c.to_number_descriptor());
							return Err(TypeError::NumberOutOfRange {
								target: <L as Promote<R>>::Output::get_type(),
								fragment: fragment.fragment(),
								descriptor,
							}
							.into());
						};

						if <<L as Promote<R>>::Output as $safe_trait>::is_zero(&rp) {
							return Err(TypeError::DivisionByZero {
								target: <L as Promote<R>>::Output::get_type(),
								fragment: fragment.fragment(),
							}
							.into());
						}

						lp.$checked_method(&rp)
							.ok_or_else(|| {
								let descriptor = self
									.target
									.as_ref()
									.and_then(|c| c.to_number_descriptor());
								TypeError::NumberOutOfRange {
									target: <L as Promote<R>>::Output::get_type(),
									fragment: fragment.fragment(),
									descriptor,
								}
								.into()
							})
							.map(Some)
					}
					ColumnSaturationStrategy::None => {
						let Some((lp, rp)) = l.checked_promote(r) else {
							return Ok(None);
						};

						match lp.$checked_method(&rp) {
							None => Ok(None),
							Some(value) => Ok(Some(value)),
						}
					}
				}
			}
		}
	};
}

pub(crate) trait FitFamily: Sized {
	fn fit_family(&self, target: &ValueType) -> Option<Self>;
}

impl FitFamily for Decimal {
	fn fit_family(&self, target: &ValueType) -> Option<Self> {
		let (Some(precision), Some(scale)) = (target.precision(), target.scale()) else {
			return Some(self.clone());
		};
		let rounded = self.round_to_scale(scale.value())?;
		(rounded.digits() <= precision.value()).then_some(rounded)
	}
}

impl EvalContext<'_> {
	pub(crate) fn fit_family<T: FitFamily>(
		&self,
		value: T,
		target: &ValueType,
		fragment: impl LazyFragment + Copy,
	) -> Result<Option<T>> {
		match (value.fit_family(target), &self.saturation_policy()) {
			(Some(fitted), _) => Ok(Some(fitted)),
			(None, ColumnSaturationStrategy::None) => Ok(None),
			(None, ColumnSaturationStrategy::Error) => Err(TypeError::NumberOutOfRange {
				target: target.clone(),
				fragment: fragment.fragment(),
				descriptor: self.target.as_ref().and_then(|c| c.to_number_descriptor()),
			}
			.into()),
		}
	}
}

impl_scalar_op!(add, SafeAdd, checked_add);
impl_scalar_op!(sub, SafeSub, checked_sub);
impl_scalar_op!(mul, SafeMul, checked_mul);
impl_scalar_divisive_op!(div, SafeDiv, checked_div);
impl_scalar_divisive_op!(remainder, SafeRemainder, checked_rem);

#[cfg(test)]
pub mod tests {
	use reifydb_value::fragment::Fragment;

	use crate::expression::context::EvalContext;

	#[test]
	fn test_add() {
		let test_instance = EvalContext::testing();
		let result = test_instance.add(&1i8, &255i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(256i128)));
	}

	#[test]
	fn test_sub() {
		let test_instance = EvalContext::testing();
		let result = test_instance.sub(&1i8, &255i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(-254i128)));
	}

	#[test]
	fn test_mul() {
		let test_instance = EvalContext::testing();
		let result = test_instance.mul(&23i8, &255i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(5865i128)));
	}

	#[test]
	fn test_div() {
		let test_instance = EvalContext::testing();
		let result = test_instance.div(&120i8, &20i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(6i128)));
	}

	#[test]
	fn test_remainder() {
		let test_instance = EvalContext::testing();
		let result = test_instance.remainder(&120i8, &21i16, Fragment::testing_empty);
		assert_eq!(result, Ok(Some(15i128)));
	}
}
