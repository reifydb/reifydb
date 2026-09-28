// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Reverse, sync::Arc};

use reifydb_column::snapshot::Schema;
use reifydb_core::{
	error::diagnostic::{internal::internal, query::no_column_snapshot},
	interface::{
		catalog::{
			column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
			object::ObjectId,
			series::Series,
		},
		resolved::ResolvedSeries,
	},
	key::{any::TaggedKey, partition::PartitionKey},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns, headers::ColumnHeaders},
};
use reifydb_store_column::store::ColumnStore;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	value::{partition::Partition, system_columns::SystemColumn},
};

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode},
		scan::{
			column_block_sequence::BlockSequenceReader, column_predicate::series_scan_predicate,
			column_prune::prune_series_snapshots,
		},
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

pub struct ColumnSeriesScanNode {
	series: ResolvedSeries,
	key_range_start: Option<u64>,
	key_range_end: Option<u64>,
	variant_tag: Option<u8>,
	partition: Option<Partition>,
	context: Arc<QueryContext>,
	headers: ColumnHeaders,
	state: ScanState,
}

impl ColumnSeriesScanNode {
	pub fn new(
		series: ResolvedSeries,
		key_range_start: Option<u64>,
		key_range_end: Option<u64>,
		variant_tag: Option<u8>,
		partition: Option<Partition>,
		context: Arc<QueryContext>,
	) -> Self {
		let def = series.def();
		let mut columns = vec![Fragment::internal(def.key.column())];
		if def.tag.is_some() {
			columns.push(Fragment::internal("tag"));
		}
		for col in def.data_columns() {
			columns.push(Fragment::internal(&col.name));
		}
		Self {
			series,
			key_range_start,
			key_range_end,
			variant_tag,
			partition,
			context,
			headers: ColumnHeaders {
				columns,
				row_numbers: true,
			},
			state: ScanState::Unopened,
		}
	}

	fn series_reads_as_empty(&self, rx: &mut Transaction<'_>, def: &Series) -> Result<bool> {
		let catalog = &self.context.services.catalog;
		let partitions = match self.partition {
			Some(partition) => vec![partition],
			None if def.partition_by.is_empty() => vec![Partition::default()],
			None => self.registered_partitions(rx, def)?,
		};
		for partition in partitions {
			let Some(metadata) = catalog.find_series_metadata(rx, def.id, partition)? else {
				continue;
			};
			if metadata.row_count > 0 {
				return Ok(false);
			}
		}
		Ok(true)
	}

	fn registered_partitions(&self, rx: &mut Transaction<'_>, def: &Series) -> Result<Vec<Partition>> {
		let mut partitions = Vec::new();
		let stream = rx.range(PartitionKey::full_scan(ObjectId::Series(def.id)), RangeScope::All, 1024)?;
		for entry in stream {
			if let TaggedKey::Partition(key) = entry?.key {
				partitions.push(key.partition);
			}
		}
		Ok(partitions)
	}

	fn open(&self, rx: &mut Transaction<'_>) -> Result<ScanState> {
		let services = &self.context.services;
		let name = self.series.fully_qualified_name();
		let def = self.series.def();

		let snapshots = match self.partition {
			Some(partition) => {
				services.catalog.list_column_snapshots_for_series_partition(rx, def.id, partition)?
			}
			None => services.catalog.list_column_snapshots_for_series(rx, def.id)?,
		};

		let snapshots_were_empty = snapshots.is_empty();

		let predicate = series_scan_predicate(def, self.key_range_start, self.key_range_end, self.variant_tag);

		let mut pruned =
			prune_series_snapshots(snapshots, self.key_range_start, self.key_range_end, predicate.as_ref());
		if snapshots_were_empty && self.series_reads_as_empty(rx, def)? {
			return Ok(ScanState::Done);
		}

		if pruned.is_empty() {
			return Err(Error(Box::new(no_column_snapshot(
				self.series.identifier().clone(),
				"series",
				&name,
			))));
		}
		pruned.sort_by_key(|b| Reverse(bucket_order(b)));

		let store = services.ioc.try_resolve::<Arc<ColumnStore>>().ok_or_else(|| {
			Error(Box::new(internal(format!(
				"column store is not registered, cannot read series {}",
				name
			))))
		})?;

		Ok(ScanState::Reading {
			reader: Box::new(
				BlockSequenceReader::new(
					store,
					pruned.iter().map(|snapshot| snapshot.id).collect(),
					self.context.batch_size as usize,
				)
				.with_predicate(predicate),
			),
			emitted: false,
		})
	}
}

fn bucket_order(snapshot: &ColumnSnapshot) -> (u64, Option<Partition>) {
	match snapshot.source {
		ColumnSnapshotSource::SeriesBucket {
			bucket_start,
			partition,
			..
		} => (bucket_start, partition),
		ColumnSnapshotSource::Table {
			..
		} => (0, None),
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

impl QueryNode for ColumnSeriesScanNode {
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
