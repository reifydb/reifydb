//! Single-state operators for FFI
//!
//! This module provides the `FFISingleStateful` trait for operators that maintain
//! a single state value, such as counters, accumulators, or running aggregates.

use reifydb_core::value::encoded::{EncodedKey, EncodedValues, EncodedValuesLayout};

use super::{FFIRawStatefulOperator, utils};
use crate::{OperatorContext, error::Result};

/// Operator with a single state value (like counters, running sums, etc.)
///
/// This trait provides a higher-level interface for operators that only need
/// a single state value. It handles key management automatically (using an empty key by default)
/// and provides convenient methods for loading, saving, and updating state.
pub trait FFISingleStateful: FFIRawStatefulOperator {
	/// Get or create the layout for state rows
	///
	/// This defines the structure of the state value, including field types
	/// and default values.
	fn layout(&self) -> EncodedValuesLayout;

	/// Key for the single state - default is empty
	///
	/// Override this if you need a custom key for your single state value.
	/// Most operators can use the default empty key.
	fn key(&self) -> EncodedKey {
		utils::empty_key()
	}

	/// Create a new state encoded with default values
	///
	/// This allocates a new state row based on the layout, initialized with default values.
	fn create_state(&self) -> EncodedValues {
		let layout = self.layout();
		layout.allocate()
	}

	/// Load the operator's single state
	///
	/// If the state doesn't exist, it will be created with default values from the layout.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	///
	/// # Returns
	///
	/// The loaded or newly created state
	fn load_state(&self, ctx: &mut OperatorContext) -> Result<EncodedValues> {
		let key = self.key();
		utils::load_or_create_row(ctx, &key, &self.layout())
	}

	/// Save the operator's single state
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `row` - The state to save
	fn save_state(&self, ctx: &mut OperatorContext, row: &EncodedValues) -> Result<()> {
		let key = self.key();
		utils::save_row(ctx, &key, row)
	}

	/// Update state with a function
	///
	/// This is a convenience method that loads the current state, applies a transformation function,
	/// saves the updated state, and returns the new state value.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `f` - Function that modifies the state. Receives the layout and mutable state row.
	///
	/// # Returns
	///
	/// The updated state after applying the function
	fn update_state<F>(&self, ctx: &mut OperatorContext, f: F) -> Result<EncodedValues>
	where
		F: FnOnce(&EncodedValuesLayout, &mut EncodedValues) -> Result<()>,
	{
		let layout = self.layout();
		let mut row = self.load_state(ctx)?;
		f(&layout, &mut row)?;
		self.save_state(ctx, &row)?;
		Ok(row)
	}

	/// Clear state
	///
	/// Removes the state value. The next call to `load_state` will create a new default value.
	fn clear_state(&self, ctx: &mut OperatorContext) -> Result<()> {
		let key = self.key();
		self.state_remove(ctx, &key)
	}
}
