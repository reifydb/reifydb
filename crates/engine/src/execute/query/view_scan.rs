// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	EncodedKey,
	interface::{EncodableKey, MultiVersionQueryTransaction, RowKey, RowKeyRange, resolved::ResolvedView},
	value::{
		column::{Columns, headers::ColumnHeaders},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_type::Fragment;
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct ViewScanNode<'a> {
	view: ResolvedView<'a>,
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	row_layout: EncodedValuesLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl<'a> ViewScanNode<'a> {
	pub fn new(view: ResolvedView<'a>, context: Arc<ExecutionContext<'a>>) -> crate::Result<Self> {
		let data = view.columns().iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
		let row_layout = EncodedValuesLayout::new(&data);

		let headers = ColumnHeaders {
			columns: view.columns().iter().map(|col| Fragment::owned_internal(&col.name)).collect(),
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

impl<'a> QueryNode<'a> for ViewScanNode<'a> {
	#[instrument(name = "query::scan::view::initialize", level = "trace", skip_all)]
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(name = "query::scan::view::next", level = "trace", skip_all)]
	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		_ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
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

		let multi_rows: Vec<_> =
			rx.range_batched(range, batch_size)?.into_iter().take(batch_size as usize).collect();

		for multi in multi_rows.into_iter() {
			if let Some(key) = RowKey::decode(&multi.key) {
				batch_rows.push(multi.values);
				row_numbers.push(key.row);
				new_last_key = Some(multi.key);
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

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}
