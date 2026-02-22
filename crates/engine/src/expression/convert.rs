// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::policy::ColumnSaturationPolicy;
use reifydb_type::{
	error::TypeError,
	fragment::Fragment,
	value::{number::safe::convert::SafeConvert, r#type::get::GetType},
};

use crate::expression::context::EvalContext;

pub trait Convert {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> reifydb_type::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType;
}

impl Convert for EvalContext<'_> {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> reifydb_type::Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		Convert::convert(&self, from, fragment)
	}
}

impl Convert for &EvalContext<'_> {
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
						return TypeError::IntegerPrecisionLoss {
							source_type: From::get_type(),
							target: To::get_type(),
							fragment: fragment.clone(),
						}
						.into();
					};

					let descriptor = self.target.as_ref().and_then(|c| c.to_number_descriptor());
					TypeError::NumberOutOfRange {
						target: To::get_type(),
						fragment: fragment.clone(),
						descriptor,
					}
					.into()
				})
				.map(Some),
			ColumnSaturationPolicy::None => match from.checked_convert() {
				None => Ok(None),
				Some(value) => Ok(Some(value)),
			},
		}
	}
}
