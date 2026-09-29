// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	fragment::LazyFragment,
	value::{column_view::ColumnView, value_type::ValueType},
};

use crate::error::CoreError;

pub fn check_digest_write(view: &ColumnView, target: &ValueType, fragment: impl LazyFragment) -> Result<()> {
	if view.none_count() == view.len() {
		return Ok(());
	}
	check_digest_write_type(view.get_type().inner_type(), target, fragment)
}

pub fn check_digest_write_type(actual: &ValueType, target: &ValueType, fragment: impl LazyFragment) -> Result<()> {
	let expected = target.inner_type();
	let involves_digest =
		matches!(actual, ValueType::Digest { .. }) || matches!(expected, ValueType::Digest { .. });
	if !involves_digest || actual == expected {
		return Ok(());
	}
	Err(CoreError::DigestWriteTypeMismatch {
		fragment: fragment.fragment(),
		expected: target.clone(),
		actual: actual.clone(),
	}
	.into())
}
