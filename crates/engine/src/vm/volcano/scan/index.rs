// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, shape::RowShape},
	interface::catalog::{id::IndexId, table::Table},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct IndexScanNode {
	_table: Table, // FIXME needs to work with different sources
	_index_id: IndexId,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	_storage_types: Vec<Type>,
	_shape: Option<RowShape>,
	_last_key: Option<EncodedKey>,
	_exhausted: bool,
}

impl IndexScanNode {
	pub fn new(table: Table, index_id: IndexId, context: Arc<QueryContext>) -> Result<Self> {
		let storage_types = table.columns.iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();

		let headers = ColumnHeaders {
			columns: table.columns.iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			_table: table,
			_index_id: index_id,
			context: Some(context),
			headers,
			_storage_types: storage_types,
			_shape: None,
			_last_key: None,
			_exhausted: false,
		})
	}
}

impl QueryNode for IndexScanNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "IndexScanNode::next() called before initialize()");
		unimplemented!()

		// 	// TODO: Update IndexScanNode to use ResolvedTable instead of Table
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
