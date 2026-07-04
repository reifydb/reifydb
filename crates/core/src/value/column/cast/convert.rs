// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	error::TypeError,
	fragment::Fragment,
	value::{number::safe::convert::SafeConvert, value_type::get::GetType},
};

use crate::interface::{
	catalog::property::{ColumnPropertyKind, ColumnSaturationStrategy, DEFAULT_COLUMN_SATURATION_STRATEGY},
	evaluate::TargetColumn,
};

pub trait Convert {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType;
}

#[derive(Clone, Copy)]
pub struct TargetConvert<'a> {
	pub target: Option<&'a TargetColumn>,
}

impl TargetConvert<'_> {
	fn saturation_policy(&self) -> ColumnSaturationStrategy {
		self.target
			.and_then(|t| {
				t.properties()
					.into_iter()
					.map(|p| {
						let ColumnPropertyKind::Saturation(policy) = p;
						policy
					})
					.next()
			})
			.unwrap_or(DEFAULT_COLUMN_SATURATION_STRATEGY.clone())
	}
}

impl Convert for TargetConvert<'_> {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		let fragment = fragment.into();
		match &self.saturation_policy() {
			ColumnSaturationStrategy::Error => from
				.checked_convert()
				.ok_or_else(|| {
					if From::get_type().is_integer() && To::get_type().is_floating_point() {
						return TypeError::IntegerPrecisionLoss {
							shape_type: From::get_type(),
							target: To::get_type(),
							fragment: fragment.clone(),
						}
						.into();
					};

					let descriptor = self.target.and_then(|c| c.to_number_descriptor());
					TypeError::NumberOutOfRange {
						target: To::get_type(),
						fragment: fragment.clone(),
						descriptor,
					}
					.into()
				})
				.map(Some),
			ColumnSaturationStrategy::None => match from.checked_convert() {
				None => Ok(None),
				Some(value) => Ok(Some(value)),
			},
		}
	}
}
