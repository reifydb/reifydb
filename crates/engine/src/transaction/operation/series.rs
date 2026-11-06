// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	series::EncodedSeriesRowBuilder,
	shape::RowShape,
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{object::ObjectId, series::Series},
		change::{Change, ChangeOrigin, Diff},
	},
	key::any::TaggedKey,
	partition::{PartitionError, partitions_of_rows},
};
use reifydb_flow::writer::series::SeriesWriter;
use reifydb_transaction::{
	interceptor::{WithInterceptors, series_row::SeriesRowInterceptor},
	transaction::Transaction,
};
use reifydb_value::value::{Value, datetime::DateTime, partition::Partition, row_number::RowNumber};
use smallvec::smallvec;

use crate::{Result, error::EngineError, transaction::operation::storage::DmlTransaction};

pub(crate) struct SeriesRows<'a> {
	pub keys: Vec<TaggedKey>,
	pub partitions: &'a [Partition],
	pub rows: Vec<EncodedSeriesRowBuilder>,
}

pub(crate) fn write_series_rows(
	txn: &mut Transaction<'_>,
	series: &Series,
	shape: &RowShape,
	write: SeriesRows<'_>,
) -> Result<Vec<EncodedBytes>> {
	let SeriesRows {
		keys,
		partitions,
		mut rows,
	} = write;
	assert_eq!(keys.len(), rows.len(), "keys/rows length mismatch");
	assert!(
		if series.partition_by.is_empty() {
			partitions.is_empty()
		} else {
			partitions.len() == rows.len()
		},
		"a partitioned series needs one partition per row, an unpartitioned series none"
	);
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	if !txn.series_row_pre_insert_interceptors().is_empty() {
		let key_cells = series_key_cells(shape, &rows);
		SeriesRowInterceptor::pre_insert(txn, series, &mut rows)?;
		let written: Vec<EncodedBytes> = rows.iter().map(|row| row.clone().freeze_bytes()).collect();
		if moved_series_partitions(series, shape, partitions, &written)? {
			return Err(PartitionError::PartitionColumnChangedByHook {
				object: ObjectId::series(series.id),
			}
			.into());
		}
		if series_key_cells(shape, &rows) != key_cells {
			return Err(EngineError::SeriesKeyChangedByHook {
				series: series.name.clone(),
				column: series.key.column().to_string(),
			}
			.into());
		}
	}

	let written: Vec<EncodedBytes> = rows.into_iter().map(|row| row.freeze_bytes()).collect();
	let written = SeriesWriter::new(&mut DmlTransaction::over(txn.reborrow()))
		.append(keys.into_iter().zip(written).collect())?
		.written;
	if !txn.series_row_post_insert_interceptors().is_empty() {
		SeriesRowInterceptor::post_insert(txn, series, &written)?;
	}
	Ok(written)
}

fn series_key_cells(shape: &RowShape, rows: &[EncodedSeriesRowBuilder]) -> Vec<Value> {
	rows.iter().map(|row| shape.get_value(row, 0)).collect()
}

fn series_partition_indices(series: &Series) -> Vec<usize> {
	let key_column = series.key.column();
	series.partition_by
		.iter()
		.map(|name| {
			if name == key_column {
				0
			} else {
				1 + series
					.data_columns()
					.position(|c| c.name == *name)
					.expect("partition column must exist (validated during planning)")
			}
		})
		.collect()
}

pub(crate) fn moved_series_partitions(
	series: &Series,
	shape: &RowShape,
	partitions: &[Partition],
	rows: &[EncodedBytes],
) -> Result<bool> {
	if series.partition_by.is_empty() {
		return Ok(false);
	}
	Ok(partitions_of_rows(shape, &series_partition_indices(series), rows)? != partitions)
}

pub(crate) fn emit_series_remove_change(txn: &mut Transaction<'_>, series: &Series, pre: RecordBatch) {
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::remove(pre)],
		changed_at: DateTime::default(),
	});
}

pub(crate) fn remove_series_rows(
	txn: &mut Transaction<'_>,
	series: &Series,
	ids: &[RowNumber],
	removals: Vec<(TaggedKey, EncodedBytes)>,
) -> Result<()> {
	assert_eq!(ids.len(), removals.len(), "ids/removals length mismatch");
	if ids.is_empty() {
		return Ok(());
	}
	if !txn.series_row_pre_delete_interceptors().is_empty() {
		SeriesRowInterceptor::pre_delete(txn, series, ids)?;
	}
	let replaced = SeriesWriter::new(&mut DmlTransaction::over(txn.reborrow())).delete(removals)?.replaced;
	if !txn.series_row_post_delete_interceptors().is_empty() {
		SeriesRowInterceptor::post_delete(txn, series, &replaced)?;
	}
	Ok(())
}
