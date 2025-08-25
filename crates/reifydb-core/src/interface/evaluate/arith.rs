// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Error, GetType, IntoFragment,
	interface::{ColumnSaturationPolicy, evaluate::EvaluationContext},
	result::error::diagnostic::number::number_out_of_range,
	return_error,
	value::{
		IsNumber,
		number::{
			Promote, SafeAdd, SafeDiv, SafeMul, SafeRemainder,
			SafeSub,
		},
	},
};

impl EvaluationContext<'_> {
	pub fn add<L, R>(
		&self,
		l: L,
		r: R,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeAdd,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        self.target_column.as_ref(),
                    ));
				};

				lp.checked_add(rp)
					.ok_or_else(|| {
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            self.target_column.as_ref(),
                        ))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return Ok(None);
				};

				match lp.checked_add(rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl EvaluationContext<'_> {
	pub fn sub<L, R>(
		&self,
		l: L,
		r: R,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeSub,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        self.target_column.as_ref(),
                    ));
				};

				lp.checked_sub(rp)
					.ok_or_else(|| {
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            self.target_column.as_ref(),
                        ))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return Ok(None);
				};

				match lp.checked_sub(rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl EvaluationContext<'_> {
	pub fn mul<L, R>(
		&self,
		l: L,
		r: R,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeMul,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        self.target_column.as_ref(),
                    ));
				};

				lp.checked_mul(rp)
					.ok_or_else(|| {
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            self.target_column.as_ref(),
                        ))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return Ok(None);
				};

				match lp.checked_mul(rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl EvaluationContext<'_> {
	pub fn div<L, R>(
		&self,
		l: L,
		r: R,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeDiv,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        self.target_column.as_ref(),
                    ));
				};

				lp.checked_div(rp)
					.ok_or_else(|| {
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            self.target_column.as_ref(),
                        ))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return Ok(None);
				};

				match lp.checked_div(rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

impl EvaluationContext<'_> {
	pub fn remainder<L, R>(
		&self,
		l: L,
		r: R,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<<L as Promote<R>>::Output>>
	where
		L: Promote<R>,
		R: IsNumber,
		<L as Promote<R>>::Output: IsNumber,
		<L as Promote<R>>::Output: SafeRemainder,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        self.target_column.as_ref(),
                    ));
				};

				lp.checked_rem(rp)
					.ok_or_else(|| {
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            self.target_column.as_ref(),
                        ))
					})
					.map(Some)
			}
			ColumnSaturationPolicy::Undefined => {
				let Some((lp, rp)) = l.checked_promote(r)
				else {
					return Ok(None);
				};

				match lp.checked_rem(rp) {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{Fragment, interface::evaluate::EvaluationContext};

	#[test]
	fn test_add() {
		let test_instance = EvaluationContext::testing();
		let result = test_instance.add(
			1i8,
			255i16,
			Fragment::testing_empty(),
		);
		assert_eq!(result, Ok(Some(256i128)));
	}

	#[test]
	fn test_sub() {
		let test_instance = EvaluationContext::testing();
		let result = test_instance.sub(
			1i8,
			255i16,
			Fragment::testing_empty(),
		);
		assert_eq!(result, Ok(Some(-254i128)));
	}

	#[test]
	fn test_mul() {
		let test_instance = EvaluationContext::testing();
		let result = test_instance.mul(
			23i8,
			255i16,
			Fragment::testing_empty(),
		);
		assert_eq!(result, Ok(Some(5865i128)));
	}

	#[test]
	fn test_div() {
		let test_instance = EvaluationContext::testing();
		let result = test_instance.div(
			120i8,
			20i16,
			Fragment::testing_empty(),
		);
		assert_eq!(result, Ok(Some(6i128)));
	}

	#[test]
	fn test_remainder() {
		let test_instance = EvaluationContext::testing();
		let result = test_instance.remainder(
			120i8,
			21i16,
			Fragment::testing_empty(),
		);
		assert_eq!(result, Ok(Some(15i128)));
	}
}
