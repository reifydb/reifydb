// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Window-based state management for FFI operators
//!
//! This module provides the `FFIWindowStateful` trait for operators that use
//! time-based or count-based windowing with state.

use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey};
use reifydb_core::schema::Schema;

use super::{FFIRawStatefulOperator, utils};
use crate::{error::Result, operator::context::OperatorContext};

/// Window-based state management for time or count-based windowing
///
/// This trait provides support for operators that partition state into windows,
/// such as sliding time windows or tumbling count windows. Each window has its own
/// state entry indexed by a window key.
///
/// Window keys should be designed to support efficient expiration. Common patterns:
/// - Time-based: Use timestamp as part of the key
/// - Count-based: Use sequence number as part of the key
/// - Composite: Combine time/count with other dimensions
pub trait FFIWindowStateful: FFIRawStatefulOperator {
	/// Get or create the schema for state rows
	///
	/// This defines the structure of each window's state.
	fn schema(&self) -> Schema;

	/// Create a new state encoded with default values
	///
	/// Allocates a new window state row based on the schema, initialized with default values.
	fn create_state(&self) -> EncodedValues {
		let schema = self.schema();
		schema.allocate()
	}

	/// Load state for a window
	///
	/// If state for this window doesn't exist, it will be created with default values.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `window_key` - The key identifying the window
	///
	/// # Returns
	///
	/// The loaded or newly created state for this window
	fn load_state(&self, ctx: &mut OperatorContext, window_key: &EncodedKey) -> Result<EncodedValues> {
		utils::load_or_create_row(ctx, window_key, &self.schema())
	}

	/// Save state for a window
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `window_key` - The key identifying the window
	/// * `row` - The state to save
	fn save_state(&self, ctx: &mut OperatorContext, window_key: &EncodedKey, row: &EncodedValues) -> Result<()> {
		utils::save_row(ctx, window_key, row)
	}

	/// Remove state for a window
	///
	/// Deletes the state associated with this window.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `window_key` - The key identifying the window
	fn remove_window(&self, ctx: &mut OperatorContext, window_key: &EncodedKey) -> Result<()> {
		self.state_remove(ctx, window_key)
	}

	/// Update state for a window with a function
	///
	/// This is a convenience method that loads the current window state,
	/// applies a transformation function, saves the updated state, and returns
	/// the new state value.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `window_key` - The key identifying the window
	/// * `f` - Function that modifies the state. Receives the schema and mutable state row.
	///
	/// # Returns
	///
	/// The updated state after applying the function
	fn update_window<F>(&self, ctx: &mut OperatorContext, window_key: &EncodedKey, f: F) -> Result<EncodedValues>
	where
		F: FnOnce(&Schema, &mut EncodedValues) -> Result<()>,
	{
		let schema = self.schema();
		let mut row = self.load_state(ctx, window_key)?;
		f(&schema, &mut row)?;
		self.save_state(ctx, window_key, &row)?;
		Ok(row)
	}
}
