// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{
	GetType, LazyFragment, SafePromote,
	diagnostic::number::number_out_of_range, error,
};

use crate::interface::{ColumnSaturationPolicy, evaluate::EvaluationContext};

pub trait Promote {
	fn promote<'a, From, To>(
		&self,
		from: From,
		fragment: impl LazyFragment<'a>,
	) -> crate::Result<Option<To>>
	where
		From: SafePromote<To>,
		To: GetType;
}

impl Promote for EvaluationContext<'_> {
	fn promote<'a, From, To>(
		&self,
		from: From,
		fragment: impl LazyFragment<'a>,
	) -> crate::Result<Option<To>>
	where
		From: SafePromote<To>,
		To: GetType,
	{
		Promote::promote(&self, from, fragment)
	}
}

impl Promote for &EvaluationContext<'_> {
	fn promote<'a, From, To>(
		&self,
		from: From,
		fragment: impl LazyFragment<'a>,
	) -> crate::Result<Option<To>>
	where
		From: SafePromote<To>,
		To: GetType,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => from
				.checked_promote()
				.ok_or_else(|| {
					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return error!(number_out_of_range(
						fragment.fragment(),
						To::get_type(),
						descriptor.as_ref(),
					));
				})
				.map(Some),
			ColumnSaturationPolicy::Undefined => {
				match from.checked_promote() {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::{Fragment, GetType, SafePromote, Type};

	use crate::{
		ColumnDescriptor,
		interface::{
			ColumnPolicyKind::Saturation,
			ColumnSaturationPolicy::{Error, Undefined},
			evaluate::{EvaluationContext, Promote},
		},
	};

	#[test]
	fn test_promote_ok() {
		let mut ctx = EvaluationContext::testing();
		ctx.target_column =
			Some(ColumnDescriptor::new()
				.with_column_type(Type::Int2));
		ctx.column_policies = vec![Saturation(Error)];

		let result = ctx
			.promote::<i8, i16>(1i8, || Fragment::testing_empty());
		assert_eq!(result, Ok(Some(1i16)));
	}

	#[test]
	fn test_promote_fail_with_error_policy() {
		let mut ctx = EvaluationContext::testing();
		ctx.target_column =
			Some(ColumnDescriptor::new()
				.with_column_type(Type::Int2));
		ctx.column_policies = vec![Saturation(Error)];

		let err = ctx
			.promote::<TestI8, TestI16>(TestI8 {}, || {
				Fragment::testing_empty()
			})
			.err()
			.unwrap();
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "NUMBER_002")
	}

	#[test]
	fn test_promote_fail_with_undefined_policy() {
		let mut ctx = EvaluationContext::testing();
		ctx.target_column =
			Some(ColumnDescriptor::new()
				.with_column_type(Type::Int2));
		ctx.column_policies = vec![Saturation(Undefined)];

		let result = ctx
			.promote::<TestI8, TestI16>(TestI8 {}, || {
				Fragment::testing_empty()
			})
			.unwrap();
		assert!(result.is_none());
	}

	pub struct TestI8 {}

	impl SafePromote<TestI16> for TestI8 {
		fn checked_promote(self) -> Option<TestI16> {
			None
		}

		fn saturating_promote(self) -> TestI16 {
			unreachable!()
		}

		fn wrapping_promote(self) -> TestI16 {
			unreachable!()
		}
	}

	pub struct TestI16 {}

	impl GetType for TestI16 {
		fn get_type() -> Type {
			Type::Int16
		}
	}
}
