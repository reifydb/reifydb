// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::policy::ColumnSaturationPolicy;
use reifydb_type::{
	error::{Error, diagnostic::number::number_out_of_range},
	fragment::LazyFragment,
	return_error,
	value::{
		is::IsNumber,
		number::{
			promote::Promote,
			safe::{add::SafeAdd, div::SafeDiv, mul::SafeMul, remainder::SafeRemainder, sub::SafeSub},
		},
		r#type::get::GetType,
	},
};

use crate::evaluate::ColumnEvaluationContext;

impl ColumnEvaluationContext<'_> {
	pub fn add<'a, L, R>(
		&self,
		l: &L,
		r: &R,
		fragment: impl LazyFragment + Copy,
	) -> reifydb_type::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeAdd,
	{
		match &self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					return_error!(number_out_of_range(
						fragment.fragment(),
						<L as Promote<R>>::Output::get_type(),
						descriptor.as_ref(),
					));
				};

				lp.checked_add(&rp)
					.ok_or_else(|| {
						let descriptor =
							self.target.as_ref().and_then(|c| c.to_number_descriptor());
						Error(number_out_of_range(
							fragment.fragment(),
							<L as Promote<R>>::Output::get_type(),
							descriptor.as_ref(),
						))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					return Ok(None);
				};

				match lp.checked_add(&rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl ColumnEvaluationContext<'_> {
	pub fn sub<'a, L, R>(
		&self,
		l: &L,
		r: &R,
		fragment: impl LazyFragment + Copy,
	) -> reifydb_type::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeSub,
	{
		match &self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					return_error!(number_out_of_range(
						fragment.fragment(),
						<L as Promote<R>>::Output::get_type(),
						descriptor.as_ref(),
					));
				};

				lp.checked_sub(&rp)
					.ok_or_else(|| {
						let descriptor =
							self.target.as_ref().and_then(|c| c.to_number_descriptor());
						Error(number_out_of_range(
							fragment.fragment(),
							<L as Promote<R>>::Output::get_type(),
							descriptor.as_ref(),
						))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					return Ok(None);
				};

				match lp.checked_sub(&rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl ColumnEvaluationContext<'_> {
	pub fn mul<'a, L, R>(
		&self,
		l: &L,
		r: &R,
		fragment: impl LazyFragment + Copy,
	) -> reifydb_type::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeMul,
	{
		match &self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					return_error!(number_out_of_range(
						fragment.fragment(),
						<L as Promote<R>>::Output::get_type(),
						descriptor.as_ref(),
					));
				};

				lp.checked_mul(&rp)
					.ok_or_else(|| {
						let descriptor =
							self.target.as_ref().and_then(|c| c.to_number_descriptor());
						Error(number_out_of_range(
							fragment.fragment(),
							<L as Promote<R>>::Output::get_type(),
							descriptor.as_ref(),
						))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					return Ok(None);
				};

				match lp.checked_mul(&rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl ColumnEvaluationContext<'_> {
	pub fn div<'a, L, R>(
		&self,
		l: &L,
		r: &R,
		fragment: impl LazyFragment + Copy,
	) -> reifydb_type::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeDiv,
	{
		match &self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					return_error!(number_out_of_range(
						fragment.fragment(),
						<L as Promote<R>>::Output::get_type(),
						descriptor.as_ref(),
					));
				};

				lp.checked_div(&rp)
					.ok_or_else(|| {
						let descriptor =
							self.target.as_ref().and_then(|c| c.to_number_descriptor());
						Error(number_out_of_range(
							fragment.fragment(),
							<L as Promote<R>>::Output::get_type(),
							descriptor.as_ref(),
						))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					return Ok(None);
				};

				match lp.checked_div(&rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl ColumnEvaluationContext<'_> {
	pub fn remainder<'a, L, R>(
		&self,
		l: &L,
		r: &R,
		fragment: impl LazyFragment + Copy,
	) -> reifydb_type::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeRemainder,
	{
		match &self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					return_error!(number_out_of_range(
						fragment.fragment(),
						<L as Promote<R>>::Output::get_type(),
						descriptor.as_ref(),
					));
				};

				lp.checked_rem(&rp)
					.ok_or_else(|| {
						let descriptor =
							self.target.as_ref().and_then(|c| c.to_number_descriptor());
						Error(number_out_of_range(
							fragment.fragment(),
							<L as Promote<R>>::Output::get_type(),
							descriptor.as_ref(),
						))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r) else {
					return Ok(None);
				};

				match lp.checked_rem(&rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::fragment::Fragment;

	use crate::evaluate::ColumnEvaluationContext;

	#[test]
	fn test_add() {
		let test_instance = ColumnEvaluationContext::testing();
		let result = test_instance.add(&1i8, &255i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(256i128)));
	}

	#[test]
	fn test_sub() {
		let test_instance = ColumnEvaluationContext::testing();
		let result = test_instance.sub(&1i8, &255i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(-254i128)));
	}

	#[test]
	fn test_mul() {
		let test_instance = ColumnEvaluationContext::testing();
		let result = test_instance.mul(&23i8, &255i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(5865i128)));
	}

	#[test]
	fn test_div() {
		let test_instance = ColumnEvaluationContext::testing();
		let result = test_instance.div(&120i8, &20i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(6i128)));
	}

	#[test]
	fn test_remainder() {
		let test_instance = ColumnEvaluationContext::testing();
		let result = test_instance.remainder(&120i8, &21i16, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(15i128)));
	}
}
