// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			series::{Series, SeriesMetadata},
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
	},
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		series_row::SeriesRowKey,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};
use smallvec::smallvec;

use crate::Result;

pub(crate) fn decode_series_storage_key(series: &Series, key: &EncodedKey, partitioned: bool) -> Option<SeriesRowKey> {
	if partitioned {
		match PartitionedRowKey::decode(key).map(|pk| pk.locator) {
			Some(RowLocator::Series {
				variant_tag,
				key,
				sequence,
			}) => Some(SeriesRowKey {
				series: series.id,
				variant_tag,
				key,
				sequence,
			}),
			_ => None,
		}
	} else {
		SeriesRowKey::decode(key)
	}
}

pub(crate) fn build_series_delete_pre_columns_from_storage(
	series: &Series,
	shape: &RowShape,
	encoded_row: &EncodedRow,
	decoded_key: &SeriesRowKey,
) -> Columns {
	let row_number = RowNumber::from(decoded_key.sequence);
	let data_values: Vec<Value> =
		series.data_columns().enumerate().map(|(i, _)| shape.get_value(encoded_row, i + 1)).collect();
	let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
	pre_col_vec.push(ColumnWithName::new(
		Fragment::internal(series.key.column()),
		series.key_column_data(vec![decoded_key.key]),
	));
	for (col_idx, col_def) in series.data_columns().enumerate() {
		let mut data = ColumnBuffer::with_capacity(col_def.constraint.get_type(), 1);
		data.push_value(data_values.get(col_idx).cloned().unwrap_or(Value::none()));
		pre_col_vec.push(ColumnWithName {
			name: Fragment::internal(&col_def.name),
			data,
		});
	}
	Columns::with_system_columns(
		pre_col_vec,
		vec![row_number],
		vec![DateTime::from_nanos(encoded_row.created_at_nanos())],
		vec![DateTime::from_nanos(encoded_row.updated_at_nanos())],
	)
}

pub(crate) fn emit_series_remove_change(txn: &mut Transaction<'_>, series: &Series, pre: Columns) {
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Shape(ShapeId::series(series.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(pre)],
		changed_at: DateTime::default(),
	});
}

pub(crate) fn remove_series_row(
	txn: &mut Transaction<'_>,
	series: &Series,
	key: &EncodedKey,
	pre_for_cdc: EncodedRow,
	was_committed: bool,
	pre: Option<Columns>,
) -> Result<()> {
	if let Some(pre) = pre {
		emit_series_remove_change(txn, series, pre);
	}
	SeriesRowInterceptor::pre_delete(txn, series)?;
	if was_committed {
		txn.mark_preexisting(key)?;
	}
	txn.unset(key, pre_for_cdc.clone())?;
	let pre_rows = [pre_for_cdc];
	SeriesRowInterceptor::post_delete(txn, series, &pre_rows)?;
	Ok(())
}

pub(crate) fn apply_series_metadata_after_delete(metadata: &mut SeriesMetadata, deleted_count: u64) {
	metadata.row_count = metadata.row_count.saturating_sub(deleted_count);
	if metadata.row_count == 0 {
		metadata.oldest_key = 0;
		metadata.newest_key = 0;
	}
}
