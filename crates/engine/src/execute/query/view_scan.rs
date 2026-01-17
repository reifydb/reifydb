// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, layout::EncodedValuesLayout},
	interface::resolved::ResolvedView,
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::standard::StandardTransaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct ViewScanNode {
	view: ResolvedView,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	row_layout: EncodedValuesLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl ViewScanNode {
	pub fn new(view: ResolvedView, context: Arc<ExecutionContext>) -> crate::Result<Self> {
		let data = view.columns().iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
		let row_layout = EncodedValuesLayout::testing(&data);

		let headers = ColumnHeaders {
			columns: view.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			view,
			context: Some(context),
			headers,
			row_layout,
			last_key: None,
			exhausted: false,
		})
	}
}

impl QueryNode for ViewScanNode {
	#[instrument(name = "query::scan::view::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(name = "query::scan::view::next", level = "trace", skip_all)]
	fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "ViewScanNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let range = RowKeyRange::scan_range(self.view.def().id.into(), self.last_key.as_ref());

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut new_last_key = None;

		let mut stream = rx.range(range, batch_size as usize)?;
		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					if let Some(key) = RowKey::decode(&multi.key) {
						batch_rows.push(multi.values);
						row_numbers.push(key.row);
						new_last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e.into()),
				None => {
					self.exhausted = true;
					break;
				}
			}
		}

		if batch_rows.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		self.last_key = new_last_key;

		let mut columns = Columns::from_view(&self.view);
		columns.append_rows(&self.row_layout, batch_rows.into_iter(), row_numbers)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
