// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	SortKey,
	interface::{Params, TableVirtualDef, expression::Expression},
};

use crate::{execute::Batch, transaction::StandardTransaction};

pub(crate) mod system;

/// Context passed to virtual table queries
pub enum TableVirtualContext<'a> {
	Basic {
		/// Query parameters
		params: Params,
	},
	PushDown {
		/// Filter conditions from filter operations
		filters: Vec<Expression<'a>>,
		/// Projection expressions from map operations (empty = select
		/// all)
		projections: Vec<Expression<'a>>,
		/// Sort keys from order operations
		order_by: Vec<SortKey>,
		/// Limit from take operations
		limit: Option<usize>,
		/// Query parameters
		params: Params,
	},
}

/// Trait for virtual table instances that follow the volcano iterator pattern
pub trait TableVirtual<'a>: Send + Sync {
	/// Initialize the virtual table iterator with context
	/// Called once before iteration begins
	fn initialize(&mut self, txn: &mut StandardTransaction<'a>, ctx: TableVirtualContext<'a>) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch<'a>>>;

	/// Get the table definition
	fn definition(&self) -> &TableVirtualDef;
}
