// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator context providing access to state and resources

use reifydb_abi::context::context::ContextFFI;
use reifydb_core::{encoded::key::EncodedKey, interface::catalog::flow::FlowNodeId};
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, row_number::RowNumber},
};

use crate::{
	catalog::Catalog,
	error::Result,
	rql::raw_rql,
	state::{State, row::RowNumberProvider},
	store::Store,
};

/// Operator context providing access to state and other resources
pub struct OperatorContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl OperatorContext {
	/// Create a new operator context from an FFI context pointer
	///
	/// # Safety
	/// The caller must ensure ctx is non-null and valid for the lifetime of this context
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	/// Get the operator ID from the FFI context
	pub fn operator_id(&self) -> FlowNodeId {
		unsafe { FlowNodeId((*self.ctx).operator_id) }
	}

	/// Get a state manager
	pub fn state(&mut self) -> State<'_> {
		State::new(self)
	}

	/// Get read-only access to the underlying store
	pub fn store(&mut self) -> Store<'_> {
		Store::new(self)
	}

	/// Get read-only access to the catalog
	pub fn catalog(&mut self) -> Catalog<'_> {
		Catalog::new(self)
	}

	/// Get or create a row number for a given key
	///
	/// This is a convenience method that creates a RowNumberProvider and
	/// delegates to its `get_or_create_row_number` method.
	///
	/// Returns `(RowNumber, is_new)` where `is_new` indicates if this is
	/// a newly created row number.
	/// ```
	pub fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		let provider = RowNumberProvider::new(self.operator_id());
		provider.get_or_create_row_number(self, key)
	}

	/// Execute an RQL statement within the current transaction.
	pub fn rql(&self, rql: &str, params: Params) -> Result<Vec<Frame>> {
		raw_rql(self, rql, params)
	}
}
