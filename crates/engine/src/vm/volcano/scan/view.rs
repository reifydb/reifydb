// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey, schema::Schema},
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
	schema: Option<Schema>,
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
			schema: None,
			last_key: None,
			exhausted: false,
		})
	}

	fn get_or_load_schema<'a>(&mut self, rx: &mut Transaction<'a>, first_row: &EncodedValues) -> Result<Schema> {
		if let Some(schema) = &self.schema {
			return Ok(schema.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("ViewScanNode context not set");
		let schema = stored_ctx.services.catalog.schema.get_or_load(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"Schema with fingerprint {:?} not found for view {}",
				fingerprint,
				self.view.def().name
			)
		})?;

		self.schema = Some(schema.clone());

		Ok(schema)
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

		// Drop the stream to release the borrow on rx
		drop(stream);
		if batch_rows.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		self.last_key = new_last_key;

		let mut columns = Columns::from_view(&self.view);
		let schema = self.get_or_load_schema(rx, &batch_rows[0])?;
		columns.append_rows(&schema, batch_rows.into_iter(), row_numbers)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
