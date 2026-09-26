// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::snapshot::Schema;
use reifydb_core::{
	error::diagnostic::{internal::internal, query::no_column_snapshot},
	interface::resolved::ResolvedTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns, headers::ColumnHeaders},
};
use reifydb_store_column::store::ColumnStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, fragment::Fragment, value::system_columns::SystemColumn};

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode},
		scan::column_block_sequence::BlockSequenceReader,
	},
};

enum ScanState {
	Unopened,
	Reading {
		reader: Box<BlockSequenceReader>,
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
		let columns: Vec<Fragment> = table.columns().iter().map(|col| Fragment::internal(&col.name)).collect();
		Self {
			table,
			context,
			headers: ColumnHeaders {
				columns,
				row_numbers: true,
			},
			state: ScanState::Unopened,
		}
	}

	fn open(&self, rx: &mut Transaction<'_>) -> Result<ScanState> {
		let services = &self.context.services;
		let name = self.table.fully_qualified_name();

		let snapshot =
			services.catalog.find_latest_column_snapshot_for_table(rx, self.table.def().id)?.ok_or_else(
				|| Error(Box::new(no_column_snapshot(self.table.identifier().clone(), "table", &name))),
			)?;

		let store = services.ioc.try_resolve::<Arc<ColumnStore>>().ok_or_else(|| {
			Error(Box::new(internal(format!("column store is not registered, cannot read table {}", name))))
		})?;

		Ok(ScanState::Reading {
			reader: Box::new(BlockSequenceReader::new(
				store,
				vec![snapshot.id],
				self.context.batch_size as usize,
			)),
			emitted: false,
		})
	}
}

fn empty_columns(schema: &Schema) -> Columns {
	let columns = schema
		.iter()
		.filter(|(name, _, _)| SystemColumn::from_name(name).is_none())
		.map(|(name, ty, _)| {
			ColumnWithName::new(
				Fragment::internal(name.clone()),
				ColumnBuilder::with_capacity(ty.clone(), 0).finish(),
			)
		})
		.collect();
	let mut columns = Columns::new(columns);
	columns.system.mark_row_numbers();
	columns
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
			emitted,
		} = &mut self.state
		else {
			return Ok(None);
		};
		match reader.next()? {
			Some(batch) => {
				*emitted = true;
				Ok(Some(batch))
			}
			None => {
				let empty = (!*emitted).then(|| reader.schema().map(empty_columns)).flatten();
				self.state = ScanState::Done;
				Ok(empty)
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
