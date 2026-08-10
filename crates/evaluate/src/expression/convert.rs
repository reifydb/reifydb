// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::cast::convert::{Convert, TargetConvert};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{number::safe::convert::SafeConvert, value_type::get::GetType},
};

use crate::expression::context::EvalContext;

impl Convert for EvalContext<'_> {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		Convert::convert(&self, from, fragment)
	}
}

impl Convert for &EvalContext<'_> {
	fn convert<From, To>(&self, from: From, fragment: impl Into<Fragment>) -> Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		TargetConvert {
			target: self.target.as_ref(),
		}
		.convert(from, fragment)
	}
}
