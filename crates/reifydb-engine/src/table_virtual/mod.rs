// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	SortKey, interface::VirtualTableDef, value::columnar::Columns,
};

mod provider;
pub mod system;
use reifydb_core::interface::{Params, Transaction, expression::Expression};

use crate::StandardTransaction;

/// Context passed to virtual table queries with pushdown operations
pub struct VirtualTableQueryContext {
	/// Filter conditions from filter operations
	pub filters: Vec<Expression>,
	/// Projection expressions from map operations (empty = select all)
	pub projections: Vec<Expression>,
	/// Sort keys from order operations
	pub order_by: Vec<SortKey>,
	/// Limit from take operations
	pub limit: Option<usize>,
	/// Query parameters
	pub params: Params,
}

/// Trait for virtual table instances that can execute queries with pushdown
/// optimization
pub trait VirtualTable<T: Transaction>: Send + Sync {
	/// Execute a query with pushdown context
	fn query(
		&self,
		ctx: VirtualTableQueryContext,
		txn: &mut StandardTransaction<T>,
	) -> crate::Result<Columns>;

	/// Get the table definition
	fn definition(&self) -> &VirtualTableDef;
}
