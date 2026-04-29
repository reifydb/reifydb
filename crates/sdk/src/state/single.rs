// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape};

use super::{FFIRawStatefulOperator, utils};
use crate::{error::Result, operator::context::OperatorContext};

/// Operator with a single state value (like counters, running sums, etc.)
///
/// This trait provides a higher-level interface for operators that only need
/// a single state value. It handles key management automatically (using an empty key by default)
/// and provides convenient methods for loading, saving, and updating state.
pub trait FFISingleStateful: FFIRawStatefulOperator {
	fn shape(&self) -> RowShape;

	fn key(&self) -> EncodedKey {
		utils::empty_key()
	}

	fn create_state(&self) -> EncodedRow {
		let shape = self.shape();
		shape.allocate()
	}

	fn load_state(&self, ctx: &mut OperatorContext) -> Result<EncodedRow> {
		let key = self.key();
		utils::load_or_create_row(ctx, &key, &self.shape())
	}

	fn save_state(&self, ctx: &mut OperatorContext, row: &EncodedRow) -> Result<()> {
		let key = self.key();
		utils::save_row(ctx, &key, row)
	}

	fn update_state<F>(&self, ctx: &mut OperatorContext, f: F) -> Result<EncodedRow>
	where
		F: FnOnce(&RowShape, &mut EncodedRow) -> Result<()>,
	{
		let shape = self.shape();
		let mut row = self.load_state(ctx)?;
		f(&shape, &mut row)?;
		self.save_state(ctx, &row)?;
		Ok(row)
	}

	fn clear_state(&self, ctx: &mut OperatorContext) -> Result<()> {
		let key = self.key();
		self.state_remove(ctx, &key)
	}
}
