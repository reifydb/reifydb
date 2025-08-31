// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	SortKey,
	interface::{
		Params, Transaction, VirtualTableDef, expression::Expression,
	},
	value::columnar::Columns,
};

use crate::StandardTransaction;

pub(crate) mod system;

/// Context passed to virtual table queries
pub enum VirtualTableContext<'a> {
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

/// Trait for virtual table instances that can execute queries with pushdown
/// optimization
pub trait VirtualTable<T: Transaction>: Send + Sync {
	/// Execute a query with pushdown context
	fn query<'a>(
		&self,
		txn: &mut StandardTransaction<'a, T>,
		ctx: VirtualTableContext<'a>,
	) -> crate::Result<Columns>;

	/// Get the table definition
	fn definition(&self) -> &VirtualTableDef;
}
