// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	reader::SnapshotReader,
	snapshot::{Schema, SystemColumn},
};
use reifydb_core::{
	error::diagnostic::{internal::internal, query::no_column_snapshot},
	interface::resolved::ResolvedTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_store_column::ColumnStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, fragment::Fragment};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

enum ScanState {
	Unopened,
	Reading {
		reader: SnapshotReader,
		schema: Schema,
		emitted: bool,
	},
	Done,
}

pub struct ColumnTableScanNode {
	table: ResolvedTable,
	context: Arc<QueryContext>,
	headers: ColumnHeaders,
	state: ScanState,
}

impl ColumnTableScanNode {
	pub fn new(table: ResolvedTable, context: Arc<QueryContext>) -> Self {
		let mut columns: Vec<Fragment> =
			table.columns().iter().map(|col| Fragment::internal(&col.name)).collect();
		columns.push(Fragment::internal(SystemColumn::CommitVersion.name()));
		Self {
			table,
			context,
			headers: ColumnHeaders {
				columns,
			},
			state: ScanState::Unopened,
		}
	}

	fn open(&self, rx: &mut Transaction<'_>) -> Result<ScanState> {
		let services = &self.context.services;
		let name = self.table.fully_qualified_name();

		let snapshot = services
			.catalog
			.find_latest_column_snapshot_for_table(rx, self.table.def().id)?
			.ok_or_else(|| Error(Box::new(no_column_snapshot(self.table.identifier().clone(), &name))))?;

		let store = services.ioc.try_resolve::<Arc<ColumnStore>>().ok_or_else(|| {
			Error(Box::new(internal(format!("column store is not registered, cannot read table {}", name))))
		})?;

		let block = store.get(snapshot.id).ok_or_else(|| {
			Error(Box::new(internal(format!(
				"column block for snapshot {} of table {} is missing from the column store",
				snapshot.id, name
			))))
		})?;

		let schema = Arc::clone(&block.schema);
		let reader = SnapshotReader::new(block, self.context.batch_size as usize);
		Ok(ScanState::Reading {
			reader,
			schema,
			emitted: false,
		})
	}
}

fn empty_columns(schema: &Schema) -> Columns {
	let columns = schema
		.iter()
		.filter(|(name, _, _)| matches!(SystemColumn::from_name(name), None | Some(SystemColumn::CommitVersion)))
		.map(|(name, ty, _)| {
			ColumnWithName::new(Fragment::internal(name.clone()), ColumnBuffer::with_capacity(ty.clone(), 0))
		})
		.collect();
	Columns::new(columns)
}

impl QueryNode for ColumnTableScanNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if matches!(self.state, ScanState::Unopened) {
			self.state = self.open(rx)?;
		}
		let ScanState::Reading {
			reader,
			schema,
			emitted,
		} = &mut self.state
		else {
			return Ok(None);
		};
		match reader.next() {
			Some(batch) => {
				*emitted = true;
				batch.map(Some)
			}
			None => {
				let empty = (!*emitted).then(|| empty_columns(schema));
				self.state = ScanState::Done;
				Ok(empty)
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
