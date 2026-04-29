// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
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
	fn shape(&self) -> RowShape;

	fn key_types(&self) -> &[Type];

	fn encode_key(&self, key_values: &[Value]) -> EncodedKey {
		// Use keycode encoding for order-preserving keys
		let mut serializer = KeySerializer::new();

		for value in key_values.iter() {
			serializer.extend_value(value);
		}

		EncodedKey::new(serializer.finish())
	}

	fn create_state(&self) -> EncodedRow {
		let shape = self.shape();
		shape.allocate()
	}

	fn load_state(&self, ctx: &mut OperatorContext, key_values: &[Value]) -> Result<EncodedRow> {
		let key = self.encode_key(key_values);
		utils::load_or_create_row(ctx, &key, &self.shape())
	}

	fn save_state(&self, ctx: &mut OperatorContext, key_values: &[Value], row: &EncodedRow) -> Result<()> {
		let key = self.encode_key(key_values);
		utils::save_row(ctx, &key, row)
	}

	fn update_state<F>(&self, ctx: &mut OperatorContext, key_values: &[Value], f: F) -> Result<EncodedRow>
	where
		F: FnOnce(&RowShape, &mut EncodedRow) -> Result<()>,
	{
		let shape = self.shape();
		let mut row = self.load_state(ctx, key_values)?;
		f(&shape, &mut row)?;
		self.save_state(ctx, key_values, &row)?;
		Ok(row)
	}

	fn remove_state(&self, ctx: &mut OperatorContext, key_values: &[Value]) -> Result<()> {
		let key = self.encode_key(key_values);
		self.state_remove(ctx, &key)
	}
}
