// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod cache;
pub mod ffi;
pub mod keyed;
pub mod row;
pub mod single;
pub mod utils;
pub mod window;

use std::ops::Bound;

use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow};

use crate::{
	error::Result,
	operator::{FFIOperator, context::OperatorContext},
};

pub struct State<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> State<'a> {
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn get(&self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		ffi::get(self.ctx, key)
	}

	pub fn set(&mut self, key: &EncodedKey, value: &EncodedRow) -> Result<()> {
		ffi::set(self.ctx, key, value)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::remove(self.ctx, key)
	}

	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(ffi::get(self.ctx, key)?.is_some())
	}

	pub fn clear(&mut self) -> Result<()> {
		ffi::clear(self.ctx)
	}

	pub fn scan_prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		ffi::prefix(self.ctx, prefix)
	}

	pub fn keys_with_prefix(&self, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		let entries = self.scan_prefix(prefix)?;
		Ok(entries.into_iter().map(|(k, _)| k).collect())
	}

	pub fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		ffi::range(self.ctx, start, end)
	}
}

pub trait FFIRawStatefulOperator: FFIOperator {
	fn state_get(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		ctx.state().get(key)
	}

	fn state_set(&self, ctx: &mut OperatorContext, key: &EncodedKey, value: &EncodedRow) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_scan_prefix(
		&self,
		ctx: &mut OperatorContext,
		prefix: &EncodedKey,
	) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		ctx.state().scan_prefix(prefix)
	}

	fn state_keys_with_prefix(&self, ctx: &mut OperatorContext, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		ctx.state().keys_with_prefix(prefix)
	}

	fn state_contains(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<bool> {
		ctx.state().contains(key)
	}

	fn state_clear(&self, ctx: &mut OperatorContext) -> Result<()> {
		ctx.state().clear()
	}

	fn state_scan_range(
		&self,
		ctx: &mut OperatorContext,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		ctx.state().range(start, end)
	}
}
