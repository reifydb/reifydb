// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::resolved::ResolvedShape,
	value::column::{columns::Columns, headers::ColumnHeaders},
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
		let result = (**self).next(rx, ctx)?;
		if let Some(ref columns) = result {
			columns.assert_invariants("QueryNode::next output");
		}
		Ok(result)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}
