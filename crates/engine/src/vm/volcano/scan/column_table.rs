// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{
	common::TimeSource,
	error::diagnostic::{internal::internal, query::no_column_snapshot},
	interface::resolved::ResolvedTable,
	value::column::{builder::ColumnBuilder, headers::ColumnHeaders},
};
use reifydb_store_column::{snapshot::Schema, store::ColumnStore};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, value::system_columns::SystemColumn};

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode},
		scan::{column_block_sequence::BlockSequenceReader, empty_scan, scan_headers, source_system_columns},
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
		let system_columns = source_system_columns(
			!table.def().partition_by.is_empty(),
			table.def().time != TimeSource::None,
			true,
		);
		let headers = scan_headers(table.columns().iter().map(|col| col.name.as_str()), &system_columns);
		Self {
			table,
			context,
			headers,
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

fn empty_columns(schema: &Schema) -> Result<RecordBatch> {
	let columns = schema
		.iter()
		.filter(|(name, _, _)| SystemColumn::from_name(name).is_none())
		.map(|(name, ty, _)| ColumnBuilder::with_capacity(ty.clone(), 0).finish(name))
		.collect();
	let system: Vec<SystemColumn> =
		schema.iter().filter_map(|(name, _, _)| SystemColumn::from_name(name)).collect();
	empty_scan(columns, &system)
}

impl QueryNode for ColumnTableScanNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
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
				let empty = (!*emitted)
					.then(|| reader.schema().map(empty_columns))
					.flatten()
					.transpose()?;
				self.state = ScanState::Done;
				Ok(empty)
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
