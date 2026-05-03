// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::Type;

#[derive(Debug, Clone)]
pub struct InputTypes(Vec<Vec<Type>>);

impl InputTypes {
	pub fn new(args: Vec<Vec<Type>>) -> Self {
		Self(args)
	}

	pub fn single(types: Vec<Type>) -> Self {
		Self(vec![types])
	}

	pub fn any() -> Self {
		Self(vec![vec![]])
	}

	pub fn numeric() -> Self {
		Self::single(vec![
			Type::Int1,
			Type::Int2,
			Type::Int4,
			Type::Int8,
			Type::Int16,
			Type::Uint1,
			Type::Uint2,
			Type::Uint4,
			Type::Uint8,
			Type::Uint16,
			Type::Float4,
			Type::Float8,
			Type::Int,
			Type::Uint,
			Type::Decimal,
		])
	}

	pub fn integer() -> Self {
		Self::single(vec![
			Type::Int1,
			Type::Int2,
			Type::Int4,
			Type::Int8,
			Type::Int16,
			Type::Uint1,
			Type::Uint2,
			Type::Uint4,
			Type::Uint8,
			Type::Uint16,
			Type::Int,
			Type::Uint,
		])
	}

	pub fn arity(&self) -> usize {
		self.0.len()
	}

	pub fn accepts(&self, position: usize, ty: &Type) -> bool {
		match self.0.get(position) {
			Some(types) => types.is_empty() || types.contains(ty),
			None => false,
		}
	}

	pub fn expected_at(&self, position: usize) -> &[Type] {
		self.0.get(position).map(|v| v.as_slice()).unwrap_or(&[])
	}
}
