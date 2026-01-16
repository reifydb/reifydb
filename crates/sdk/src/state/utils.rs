// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Utility functions for stateful FFI operators
//!
//! This module provides helper functions for working with state in FFI operators,
//! mirroring the functionality available to internal operators.

use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey, layout::EncodedValuesLayout};

use crate::{error::Result, operator::context::OperatorContext};

/// Create an empty key for single-state operators
///
/// This is useful for operators that only need a single state value
/// (like counters or accumulators).
///
/// # Example
///
/// ```ignore
/// let key = empty_key();
/// let value = ctx.state().get(&key)?;
/// ```
pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

/// Load state for a key, creating with default values if it doesn't exist
///
/// This is a common pattern where you want to ensure a state row exists,
/// initializing it with defaults if it's the first access.
///
/// # Arguments
///
/// * `ctx` - The operator context
/// * `key` - The key to load
/// * `layout` - The layout defining the structure and default values
///
/// # Example
///
/// ```ignore
/// let layout = EncodedValuesLayout::new(&[Type::Int32, Type::Float8]);
/// let row = load_or_create_row(ctx, &key, &layout)?;
/// ```
pub fn load_or_create_row(
	ctx: &mut OperatorContext,
	key: &EncodedKey,
	layout: &EncodedValuesLayout,
) -> Result<EncodedValues> {
	match ctx.state().get(key)? {
		Some(row) => Ok(row),
		None => Ok(layout.allocate()),
	}
}

/// Save a state row
///
/// This is a convenience wrapper around `ctx.state().set()` for saving
/// encoded values.
///
/// # Arguments
///
/// * `ctx` - The operator context
/// * `key` - The key to save under
/// * `row` - The encoded values to save
///
/// # Example
///
/// ```ignore
/// let row = EncodedValues::new(data);
/// save_row(ctx, &key, row)?;
/// ```
pub fn save_row(ctx: &mut OperatorContext, key: &EncodedKey, row: &EncodedValues) -> Result<()> {
	ctx.state().set(key, row)
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_empty_key() {
		let key = empty_key();
		assert!(key.as_bytes().is_empty());
	}

	#[test]
	fn test_empty_key_consistency() {
		let key1 = empty_key();
		let key2 = empty_key();
		assert_eq!(key1.as_bytes(), key2.as_bytes());
	}
}
