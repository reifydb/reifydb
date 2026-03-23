// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::Type;

/// Describes accepted input type combinations for a function.
///
/// Each entry in the outer Vec represents one argument position.
/// The inner Vec lists the valid types for that position.
/// An empty inner Vec means any type is accepted at that position.
#[derive(Debug, Clone)]
pub struct InputTypes(Vec<Vec<Type>>);

impl InputTypes {
	pub fn new(args: Vec<Vec<Type>>) -> Self {
		Self(args)
	}

	/// Single argument accepting specific types.
	pub fn single(types: Vec<Type>) -> Self {
		Self(vec![types])
	}

	/// Single argument accepting any type.
	pub fn any() -> Self {
		Self(vec![vec![]])
	}

	/// Single argument accepting all numeric types.
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

	/// Single argument accepting all integer types (signed + unsigned).
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

	/// Number of argument positions.
	pub fn arity(&self) -> usize {
		self.0.len()
	}

	/// Checks whether `ty` is accepted at `position`.
	pub fn accepts(&self, position: usize, ty: &Type) -> bool {
		match self.0.get(position) {
			Some(types) => types.is_empty() || types.contains(ty),
			None => false,
		}
	}

	/// Returns the accepted types at a position (for error messages).
	pub fn expected_at(&self, position: usize) -> &[Type] {
		self.0.get(position).map(|v| v.as_slice()).unwrap_or(&[])
	}
}
