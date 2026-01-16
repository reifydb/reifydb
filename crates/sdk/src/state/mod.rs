// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! State management utilities for operators

pub mod cache;
pub mod ffi;
pub mod keyed;
pub mod row;
pub mod single;
pub mod utils;
pub mod window;

use std::ops::Bound;

use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey};

use crate::{
	error::Result,
	operator::{FFIOperator, context::OperatorContext},
};

/// State manager providing state operations with EncodedKey and EncodedValues
pub struct State<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> State<'a> {
	/// Create a new state manager
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	/// Get a value from state by key
	pub fn get(&self, key: &EncodedKey) -> Result<Option<EncodedValues>> {
		ffi::raw_state_get(self.ctx, key)
	}

	/// Set a value in state by key
	pub fn set(&mut self, key: &EncodedKey, value: &EncodedValues) -> Result<()> {
		ffi::raw_state_set(self.ctx, key, value)
	}

	/// Remove a value from state by key
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::raw_state_remove(self.ctx, key)
	}

	/// Check if a key exists in state
	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(ffi::raw_state_get(self.ctx, key)?.is_some())
	}

	/// Clear all state for this operator
	pub fn clear(&mut self) -> Result<()> {
		ffi::raw_state_clear(self.ctx)
	}

	/// Scan state entries with a given key prefix
	pub fn scan_prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		ffi::raw_state_prefix(self.ctx, prefix)
	}

	/// Get all keys with a given prefix
	pub fn keys_with_prefix(&self, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		let entries = self.scan_prefix(prefix)?;
		Ok(entries.into_iter().map(|(k, _)| k).collect())
	}

	/// Scan state entries within a given range
	pub fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		ffi::raw_state_range(self.ctx, start, end)
	}
}

/// Raw Stateful operations for FFI operators
///
/// This trait provides low-level key-value state operations for FFI operators.
/// It mirrors the internal `RawStatefulOperator` trait but works through the FFI boundary.
///
/// # Example
///
/// ```ignore
/// impl FFIRawStatefulOperator for MyOperator {}
///
/// // In your operator implementation:
/// fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
///     let key = EncodedKey::new(b"counter".to_vec());
///     let value = self.state_get(ctx, &key)?;
///     // ... use state
/// }
/// ```
pub trait FFIRawStatefulOperator: FFIOperator {
	/// Get raw bytes for a key
	fn state_get(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<Option<EncodedValues>> {
		ctx.state().get(key)
	}

	/// Set raw bytes for a key
	fn state_set(&self, ctx: &mut OperatorContext, key: &EncodedKey, value: &EncodedValues) -> Result<()> {
		ctx.state().set(key, value)
	}

	/// Remove a key
	fn state_remove(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<()> {
		ctx.state().remove(key)
	}

	/// Scan all keys with a prefix
	fn state_scan_prefix(
		&self,
		ctx: &mut OperatorContext,
		prefix: &EncodedKey,
	) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		ctx.state().scan_prefix(prefix)
	}

	/// Get all keys with a prefix
	fn state_keys_with_prefix(&self, ctx: &mut OperatorContext, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		ctx.state().keys_with_prefix(prefix)
	}

	/// Check if a key exists
	fn state_contains(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<bool> {
		ctx.state().contains(key)
	}

	/// Clear all state for this operator
	fn state_clear(&self, ctx: &mut OperatorContext) -> Result<()> {
		ctx.state().clear()
	}

	/// Scan all keys within a range
	fn state_scan_range(
		&self,
		ctx: &mut OperatorContext,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		ctx.state().range(start, end)
	}
}
