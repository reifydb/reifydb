// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	interface::{catalog::property::ColumnSaturationStrategy, evaluate::TargetColumn},
	value::column::cast::convert::TargetConvert,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Overflow {
	#[default]
	Target,
	Error,
	None,
	Saturate,
	Wrap,
}

#[derive(Debug, Clone, Default)]
pub struct ArithContext {
	pub target: Option<TargetColumn>,
	pub overflow: Overflow,
}

impl ArithContext {
	pub fn saturation_policy(&self) -> ColumnSaturationStrategy {
		TargetConvert {
			target: self.target.as_ref(),
		}
		.saturation_policy()
	}
}
