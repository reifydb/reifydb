// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::ValueType;

#[derive(Debug, Clone)]
pub struct InputTypes(Vec<Vec<ValueType>>);

impl InputTypes {
	pub fn single(types: Vec<ValueType>) -> Self {
		Self(vec![types])
	}

	pub fn numeric() -> Self {
		Self::single(vec![
			ValueType::Int1,
			ValueType::Int2,
			ValueType::Int4,
			ValueType::Int8,
			ValueType::Int16,
			ValueType::Uint1,
			ValueType::Uint2,
			ValueType::Uint4,
			ValueType::Uint8,
			ValueType::Uint16,
			ValueType::Float4,
			ValueType::Float8,
			ValueType::Int,
			ValueType::Uint,
			ValueType::Decimal,
		])
	}

	pub fn expected_at(&self, position: usize) -> &[ValueType] {
		self.0.get(position).map(|v| v.as_slice()).unwrap_or(&[])
	}
}
