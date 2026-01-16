// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::policy::ColumnSaturationPolicy;
use reifydb_type::{
	error,
	error::diagnostic::number::{integer_precision_loss, number_out_of_range},
	fragment::Fragment,
	value::{number::safe::convert::SafeConvert, r#type::get::GetType},
};

use crate::evaluate::ColumnEvaluationContext;

pub trait Convert {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> reifydb_type::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType;
}

impl Convert for ColumnEvaluationContext<'_> {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> reifydb_type::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		Convert::convert(&self, from, fragment)
	}
}

impl Convert for &ColumnEvaluationContext<'_> {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> reifydb_type::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		let fragment = fragment.into();
		match &self.saturation_policy() {
			ColumnSaturationPolicy::Error => from
				.checked_convert()
				.ok_or_else(|| {
					if From::get_type().is_integer() && To::get_type().is_floating_point() {
						return error!(integer_precision_loss(
							fragment.clone(),
							From::get_type(),
							To::get_type(),
						));
					};

					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					return error!(number_out_of_range(
						fragment.clone(),
						To::get_type(),
						descriptor.as_ref(),
					));
				})
				.map(Some),
			ColumnSaturationPolicy::Undefined => match from.checked_convert() {
				None => Ok(None),
				Some(value) => Ok(Some(value)),
			},
		}
	}
}
