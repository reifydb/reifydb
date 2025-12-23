// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	SortKey,
	interface::{Params, TableVirtualDef},
};
use reifydb_rql::expression::Expression;

use crate::{execute::Batch, transaction::StandardTransaction};

mod adapter;
mod factory;
mod registry;
pub(crate) mod system;
pub mod user;

pub use factory::VirtualTableFactory;
pub use registry::{IteratorVirtualTableFactory, SimpleVirtualTableFactory, TableVirtualUserRegistry};
pub use user::{
	TableVirtualUser, TableVirtualUserColumnDef, TableVirtualUserIterator, TableVirtualUserPushdownContext,
};

/// Context passed to virtual table queries
pub enum TableVirtualContext {
	Basic {
		/// Query parameters
		params: Params,
	},
	PushDown {
		/// Filter conditions from filter operations
		filters: Vec<Expression>,
		/// Projection expressions from map operations (empty = select
		/// all)
		projections: Vec<Expression>,
		/// Sort keys from order operations
		order_by: Vec<SortKey>,
		/// Limit from take operations
		limit: Option<usize>,
		/// Query parameters
		params: Params,
	},
}

/// Trait for virtual table instances that follow the volcano iterator pattern
#[async_trait]
pub trait TableVirtual: Send + Sync {
	/// Initialize the virtual table iterator with context
	/// Called once before iteration begins
	async fn initialize<'a>(
		&mut self,
		txn: &mut StandardTransaction<'a>,
		ctx: TableVirtualContext,
	) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	async fn next<'a>(&mut self, txn: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch>>;

	/// Get the table definition
	fn definition(&self) -> &TableVirtualDef;
}
