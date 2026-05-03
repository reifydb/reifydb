// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::resolved::ResolvedShape,
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

pub trait QueryNode: Send + Sync {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()>;

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>>;

	fn next_lazy<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<LazyBatch>> {
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders>;
}

#[derive(Clone)]
pub struct QueryContext {
	pub services: Arc<Services>,
	pub source: Option<ResolvedShape>,
	pub batch_size: u64,
	pub params: Params,
	pub symbols: SymbolTable,
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
