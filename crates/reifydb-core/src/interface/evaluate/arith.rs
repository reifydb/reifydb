// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{
	Error, GetType, IntoFragment, IsNumber, Promote, SafeAdd, SafeDiv,
	SafeMul, SafeRemainder, SafeSub,
	diagnostic::number::number_out_of_range, return_error,
};

use crate::interface::{ColumnSaturationPolicy, evaluate::EvaluationContext};

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
					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        descriptor.as_ref(),
                    ));
				};

				lp.checked_add(rp)
					.ok_or_else(|| {
						let descriptor = self
							.target_column
							.as_ref()
							.map(|c| {
								c.to_number_range_descriptor()
							});
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            descriptor.as_ref(),
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
					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        descriptor.as_ref(),
                    ));
				};

				lp.checked_sub(rp)
					.ok_or_else(|| {
						let descriptor = self
							.target_column
							.as_ref()
							.map(|c| {
								c.to_number_range_descriptor()
							});
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            descriptor.as_ref(),
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
					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        descriptor.as_ref(),
                    ));
				};

				lp.checked_mul(rp)
					.ok_or_else(|| {
						let descriptor = self
							.target_column
							.as_ref()
							.map(|c| {
								c.to_number_range_descriptor()
							});
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            descriptor.as_ref(),
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
					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        descriptor.as_ref(),
                    ));
				};

				lp.checked_div(rp)
					.ok_or_else(|| {
						let descriptor = self
							.target_column
							.as_ref()
							.map(|c| {
								c.to_number_range_descriptor()
							});
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            descriptor.as_ref(),
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
					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return_error!(number_out_of_range(
                        fragment,
                        <L as Promote<R>>::Output::get_type(),
                        descriptor.as_ref(),
                    ));
				};

				lp.checked_rem(rp)
					.ok_or_else(|| {
						let descriptor = self
							.target_column
							.as_ref()
							.map(|c| {
								c.to_number_range_descriptor()
							});
						Error(number_out_of_range(
                            fragment,
                            <L as Promote<R>>::Output::get_type(),
                            descriptor.as_ref(),
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
	use reifydb_type::Fragment;

	use crate::interface::evaluate::EvaluationContext;

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
