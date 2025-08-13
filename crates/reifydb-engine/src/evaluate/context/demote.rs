// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	GetType, IntoOwnedSpan, error, interface::ColumnSaturationPolicy,
	result::error::diagnostic::number::number_out_of_range,
	value::number::SafeDemote,
};

use crate::evaluate::EvaluationContext;

pub trait Demote {
	fn demote<From, To>(
		&self,
		from: From,
		span: impl IntoOwnedSpan,
	) -> crate::Result<Option<To>>
	where
		From: SafeDemote<To>,
		To: GetType;
}

impl Demote for EvaluationContext<'_> {
	fn demote<From, To>(
		&self,
		from: From,
		span: impl IntoOwnedSpan,
	) -> crate::Result<Option<To>>
	where
		From: SafeDemote<To>,
		To: GetType,
	{
		Demote::demote(&self, from, span)
	}
}

impl Demote for &EvaluationContext<'_> {
	fn demote<From, To>(
		&self,
		from: From,
		span: impl IntoOwnedSpan,
	) -> crate::Result<Option<To>>
	where
		From: SafeDemote<To>,
		To: GetType,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => from
				.checked_demote()
				.ok_or_else(|| {
					return error!(number_out_of_range(
						span.into_span(),
						To::get_type(),
						self.target_column.as_ref(),
					));
				})
				.map(Some),
			ColumnSaturationPolicy::Undefined => {
				match from.checked_demote() {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		ColumnDescriptor, GetType, OwnedSpan, Type,
		interface::{
			ColumnPolicyKind::Saturation,
			ColumnSaturationPolicy::{Error, Undefined},
		},
		value::number::SafeDemote,
	};

	use crate::evaluate::{Demote, EvaluationContext};

	#[test]
	fn test_demote_ok() {
		let mut ctx = EvaluationContext::testing();
		ctx.target_column =
			Some(ColumnDescriptor::new()
				.with_column_type(Type::Int1));
		ctx.column_policies = vec![Saturation(Error)];

		let result = ctx
			.demote::<i16, i8>(1i16, || OwnedSpan::testing_empty());
		assert_eq!(result, Ok(Some(1i8)));
	}

	#[test]
	fn test_demote_fail_with_error_policy() {
		let mut ctx = EvaluationContext::testing();
		ctx.target_column =
			Some(ColumnDescriptor::new()
				.with_column_type(Type::Int1));
		ctx.column_policies = vec![Saturation(Error)];

		let err = ctx
			.demote::<TestI16, TestI8>(TestI16 {}, || {
				OwnedSpan::testing_empty()
			})
			.err()
			.unwrap();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "NUMBER_002");
	}

	#[test]
	fn test_demote_fail_with_undefined_policy() {
		let mut ctx = EvaluationContext::testing();
		ctx.target_column =
			Some(ColumnDescriptor::new()
				.with_column_type(Type::Int1));
		ctx.column_policies = vec![Saturation(Undefined)];

		let result = ctx
			.demote::<TestI16, TestI8>(TestI16 {}, || {
				OwnedSpan::testing_empty()
			})
			.unwrap();
		assert!(result.is_none());
	}

	pub struct TestI16 {}

	pub struct TestI8 {}

	impl GetType for TestI8 {
		fn get_type() -> Type {
			Type::Int8
		}
	}

	impl SafeDemote<TestI8> for TestI16 {
		fn checked_demote(self) -> Option<TestI8> {
			None
		}

		fn saturating_demote(self) -> TestI8 {
			unreachable!()
		}

		fn wrapping_demote(self) -> TestI8 {
			unreachable!()
		}
	}
}
