// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::resolved::ResolvedPrimitive,
	value::{
		batch::lazy::LazyBatch,
		column::{columns::Columns, headers::ColumnHeaders},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{params::Params, value::identity::IdentityId};

use crate::{
	Result,
	vm::{services::Services, stack::SymbolTable},
};

/// Unified trait for query execution nodes following the volcano iterator pattern
pub(crate) trait QueryNode: Send + Sync {
	/// Initialize the operator with execution context
	/// Called once before iteration begins
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	/// Returns None when exhausted
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>>;

	/// Get the next batch as a LazyBatch for deferred materialization
	/// Returns None if this node doesn't support lazy evaluation or is exhausted
	/// Default implementation returns None (falls back to materialized evaluation)
	fn next_lazy<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<LazyBatch>> {
		Ok(None)
	}

	/// Get the headers of columns this node produces
	fn headers(&self) -> Option<ColumnHeaders>;
}

#[derive(Clone)]
pub struct QueryContext {
	pub services: Arc<Services>,
	pub source: Option<ResolvedPrimitive>,
	pub batch_size: u64,
	pub params: Params,
	pub stack: SymbolTable,
	pub identity: IdentityId,
}

impl QueryNode for Box<dyn QueryNode> {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		(**self).initialize(rx, ctx)
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		(**self).next(rx, ctx)
	}

	fn next_lazy<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<LazyBatch>> {
		(**self).next_lazy(rx, ctx)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}
