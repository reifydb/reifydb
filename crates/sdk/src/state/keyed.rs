// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Keyed-state operators for FFI
//!
//! This module provides the `FFIKeyedStateful` trait for operators that maintain
//! multiple state values indexed by keys, such as group-by aggregations.

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey, layout::EncodedValuesLayout},
	util::encoding::keycode::serializer::KeySerializer,
};
use reifydb_type::value::{Value, r#type::Type};

use super::{FFIRawStatefulOperator, utils};
use crate::{error::Result, operator::context::OperatorContext};

/// Operator with multiple keyed state values (for aggregations, grouping, etc.)
///
/// This trait provides a higher-level interface for operators that need to maintain
/// separate state values for different keys. This is commonly used for group-by operations
/// where each group needs its own aggregate state.
///
/// Keys are encoded using order-preserving encoding to maintain sort order.
pub trait FFIKeyedStateful: FFIRawStatefulOperator {
	/// Get or create the layout for state rows
	///
	/// This defines the structure of each state value associated with a key.
	fn layout(&self) -> EncodedValuesLayout;

	/// Schema for keys - defines the types of the key components
	///
	/// Keys can be composite (multiple values). For example, grouping by
	/// (customer_id, product_id) would return `&[Type::Int32, Type::Int32]`.
	fn key_types(&self) -> &[Type];

	/// Create EncodedKey from Values
	///
	/// Encodes key values using order-preserving encoding, which maintains
	/// sort order and allows efficient range queries.
	///
	/// # Arguments
	///
	/// * `key_values` - The values that form the key
	///
	/// # Returns
	///
	/// An encoded key that can be used for state operations
	fn encode_key(&self, key_values: &[Value]) -> EncodedKey {
		// Use keycode encoding for order-preserving keys
		let mut serializer = KeySerializer::new();

		for value in key_values.iter() {
			serializer.extend_value(value);
		}

		EncodedKey::new(serializer.finish())
	}

	/// Create a new state encoded with default values
	///
	/// Allocates a new state row based on the layout, initialized with default values.
	fn create_state(&self) -> EncodedValues {
		let layout = self.layout();
		layout.allocate()
	}

	/// Load state for a specific key
	///
	/// If state for this key doesn't exist, it will be created with default values.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `key_values` - The values that form the key
	///
	/// # Returns
	///
	/// The loaded or newly created state for this key
	fn load_state(&self, ctx: &mut OperatorContext, key_values: &[Value]) -> Result<EncodedValues> {
		let key = self.encode_key(key_values);
		utils::load_or_create_row(ctx, &key, &self.layout())
	}

	/// Save state for a specific key
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `key_values` - The values that form the key
	/// * `row` - The state to save
	fn save_state(&self, ctx: &mut OperatorContext, key_values: &[Value], row: &EncodedValues) -> Result<()> {
		let key = self.encode_key(key_values);
		utils::save_row(ctx, &key, row)
	}

	/// Update state for a key with a function
	///
	/// This is a convenience method that loads the current state for a key,
	/// applies a transformation function, saves the updated state, and returns
	/// the new state value.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `key_values` - The values that form the key
	/// * `f` - Function that modifies the state. Receives the layout and mutable state row.
	///
	/// # Returns
	///
	/// The updated state after applying the function
	fn update_state<F>(&self, ctx: &mut OperatorContext, key_values: &[Value], f: F) -> Result<EncodedValues>
	where
		F: FnOnce(&EncodedValuesLayout, &mut EncodedValues) -> Result<()>,
	{
		let layout = self.layout();
		let mut row = self.load_state(ctx, key_values)?;
		f(&layout, &mut row)?;
		self.save_state(ctx, key_values, &row)?;
		Ok(row)
	}

	/// Remove state for a key
	///
	/// Deletes the state associated with this key.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context
	/// * `key_values` - The values that form the key
	fn remove_state(&self, ctx: &mut OperatorContext, key_values: &[Value]) -> Result<()> {
		let key = self.encode_key(key_values);
		self.state_remove(ctx, &key)
	}
}
