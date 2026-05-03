// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape};

use super::{FFIRawStatefulOperator, utils};
use crate::{error::Result, operator::context::OperatorContext};

pub trait FFIWindowStateful: FFIRawStatefulOperator {
	fn shape(&self) -> RowShape;

	fn create_state(&self) -> EncodedRow {
		let shape = self.shape();
		shape.allocate()
	}

	fn load_state(&self, ctx: &mut OperatorContext, window_key: &EncodedKey) -> Result<EncodedRow> {
		utils::load_or_create_row(ctx, window_key, &self.shape())
	}

	fn save_state(&self, ctx: &mut OperatorContext, window_key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		utils::save_row(ctx, window_key, row)
	}

	fn remove_window(&self, ctx: &mut OperatorContext, window_key: &EncodedKey) -> Result<()> {
		self.state_remove(ctx, window_key)
	}

	fn update_window<F>(&self, ctx: &mut OperatorContext, window_key: &EncodedKey, f: F) -> Result<EncodedRow>
	where
		F: FnOnce(&RowShape, &mut EncodedRow) -> Result<()>,
	{
		let shape = self.shape();
		let mut row = self.load_state(ctx, window_key)?;
		f(&shape, &mut row)?;
		self.save_state(ctx, window_key, &row)?;
		Ok(row)
	}
}
