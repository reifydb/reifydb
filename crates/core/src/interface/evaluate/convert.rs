// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{
	GetType, IntoFragment, SafeConvert,
	diagnostic::number::{integer_precision_loss, number_out_of_range},
	error,
};

use crate::interface::{ColumnSaturationPolicy, evaluate::EvaluationContext};

pub trait Convert {
	fn convert<From, To>(
		&self,
		from: From,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType;
}

impl Convert for EvaluationContext<'_> {
	fn convert<From, To>(
		&self,
		from: From,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		Convert::convert(&self, from, fragment)
	}
}

impl Convert for &EvaluationContext<'_> {
	fn convert<From, To>(
		&self,
		from: From,
		fragment: impl IntoFragment<'static>,
	) -> crate::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		match self.saturation_policy() {
			ColumnSaturationPolicy::Error => from
				.checked_convert()
				.ok_or_else(|| {
					if From::get_type().is_integer()
						&& To::get_type()
							.is_floating_point()
					{
						return error!(
							integer_precision_loss(
								fragment,
								From::get_type(
								),
								To::get_type(),
							)
						);
					};

					let descriptor = self
						.target_column
						.as_ref()
						.map(|c| {
							c.to_number_range_descriptor()
						});
					return error!(number_out_of_range(
						fragment,
						To::get_type(),
						descriptor.as_ref(),
					));
				})
				.map(Some),
			ColumnSaturationPolicy::Undefined => {
				match from.checked_convert() {
					None => Ok(None),
					Some(value) => Ok(Some(value)),
				}
			}
		}
	}
}
