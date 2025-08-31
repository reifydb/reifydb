// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	SortKey,
	interface::{
		Params, TableVirtualDef, Transaction, expression::Expression,
	},
};

use crate::{StandardTransaction, execute::Batch};

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
pub trait TableVirtual<'a, T: Transaction>: Send + Sync {
	/// Initialize the virtual table iterator with context
	/// Called once before iteration begins
	fn initialize(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
		ctx: TableVirtualContext<'a>,
	) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	fn next(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>>;

	/// Get the table definition
	fn definition(&self) -> &TableVirtualDef;
}
