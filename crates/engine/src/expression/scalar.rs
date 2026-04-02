// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::property::ColumnSaturationPolicy;
use reifydb_type::{
	Result,
	error::TypeError,
	fragment::LazyFragment,
	value::{
		is::IsNumber,
		number::{
			promote::Promote,
			safe::{add::SafeAdd, div::SafeDiv, mul::SafeMul, remainder::SafeRemainder, sub::SafeSub},
		},
		r#type::get::GetType,
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
					ColumnSaturationPolicy::Error => {
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
					ColumnSaturationPolicy::None => {
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

impl_scalar_op!(add, SafeAdd, checked_add);
impl_scalar_op!(sub, SafeSub, checked_sub);
impl_scalar_op!(mul, SafeMul, checked_mul);
impl_scalar_op!(div, SafeDiv, checked_div);
impl_scalar_op!(remainder, SafeRemainder, checked_rem);

#[cfg(test)]
pub mod tests {
	use reifydb_type::fragment::Fragment;

	use crate::expression::context::EvalContext;

	#[test]
	fn test_add() {
		let test_instance = EvalContext::testing();
		let result = test_instance.add(&1i8, &255i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(256i128)));
	}

	#[test]
	fn test_sub() {
		let test_instance = EvalContext::testing();
		let result = test_instance.sub(&1i8, &255i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(-254i128)));
	}

	#[test]
	fn test_mul() {
		let test_instance = EvalContext::testing();
		let result = test_instance.mul(&23i8, &255i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(5865i128)));
	}

	#[test]
	fn test_div() {
		let test_instance = EvalContext::testing();
		let result = test_instance.div(&120i8, &20i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(6i128)));
	}

	#[test]
	fn test_remainder() {
		let test_instance = EvalContext::testing();
		let result = test_instance.remainder(&120i8, &21i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(15i128)));
	}
}
