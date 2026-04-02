// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	interface::resolved::ResolvedView,
	internal_error,
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct ViewScanNode {
	view: ResolvedView,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl ViewScanNode {
	pub fn new(view: ResolvedView, context: Arc<QueryContext>) -> Result<Self> {
		let headers = ColumnHeaders {
			columns: view.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			view,
			context: Some(context),
			headers,
			shape: None,
			last_key: None,
			exhausted: false,
		})
	}

	fn get_or_load_shape<'a>(&mut self, rx: &mut Transaction<'a>, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("ViewScanNode context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"RowShape with fingerprint {:?} not found for view {}",
				fingerprint,
				self.view.def().name()
			)
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}
}

impl QueryNode for ViewScanNode {
	#[instrument(name = "volcano::scan::view::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(name = "volcano::scan::view::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "ViewScanNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let range = RowKeyRange::scan_range(self.view.def().underlying_id(), self.last_key.as_ref());

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut new_last_key = None;

		let mut stream = rx.range(range, batch_size as usize)?;
		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					if let Some(key) = RowKey::decode(&multi.key) {
						batch_rows.push(multi.row);
						row_numbers.push(key.row);
						new_last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e),
				None => {
					self.exhausted = true;
					break;
				}
			}
		}

		// Drop the stream to release the borrow on rx
		drop(stream);
		if batch_rows.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				// Empty view: return empty columns with correct types to preserve shape
				return Ok(Some(Columns::from_resolved_view(&self.view)));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		let mut columns = Columns::from_resolved_view(&self.view);
		let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
		columns.append_rows(&shape, batch_rows.into_iter(), row_numbers)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
