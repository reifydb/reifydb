// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{Result, fragment::LazyFragment, value::value_type::ValueType};

use crate::{error::CoreError, value::column::buffer::ColumnBuffer};

impl ColumnBuffer {
	pub fn check_digest_write(&self, target: &ValueType, fragment: impl LazyFragment) -> Result<()> {
		if self.none_count() == self.len() {
			return Ok(());
		}
		check_digest_write_type(self.get_type().inner_type(), target, fragment)
	}
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
