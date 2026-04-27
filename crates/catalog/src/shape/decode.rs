// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Decode row deltas into columnar `Diff` objects.
//!
//! Uses fingerprint-based shape lookup from `MaterializedCatalog` to convert
//! `EncodedRow` into `Row`s and then into `Columns`.

use std::sync::Arc;

use reifydb_core::{
	encoded::row::{EncodedRow, SHAPE_HEADER_SIZE},
	interface::change::Diff,
	row::Row,
	value::column::{buffer::pool::ColumnBufferPool, columns::Columns},
};
use reifydb_type::value::row_number::RowNumber;
use tracing::warn;

use crate::materialized::MaterializedCatalog;

/// Try to decode an EncodedRow into a Row using the materialized catalog.
/// Returns None if the values are too short or the shape is not in the cache.
fn decode_row(catalog: &MaterializedCatalog, row_number: RowNumber, row: EncodedRow) -> Option<Row> {
	if row.len() < SHAPE_HEADER_SIZE {
		warn!("EncodedRow too short for shape fingerprint ({} < {})", row.len(), SHAPE_HEADER_SIZE);
		return None;
	}
	let fingerprint = row.fingerprint();
	let shape = catalog.find_row_shape(fingerprint);
	match shape {
		Some(shape) => Some(Row {
			number: row_number,
			encoded: row,
			shape,
		}),
		None => {
			warn!(?fingerprint, "RowShape not found in cache for row decode");
			None
		}
	}
}

/// Build an insert Diff from a row delta.
pub fn build_insert_diff(catalog: &MaterializedCatalog, row_number: RowNumber, post: EncodedRow) -> Option<Diff> {
	let row = decode_row(catalog, row_number, post)?;
	Some(Diff::insert(Columns::from_row(&row)))
}

/// Build an update Diff from a row delta with pre and post values.
pub fn build_update_diff(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	pre: EncodedRow,
	post: EncodedRow,
) -> Option<Diff> {
	let pre_row = decode_row(catalog, row_number, pre)?;
	let post_row = decode_row(catalog, row_number, post)?;
	Some(Diff::update(Columns::from_row(&pre_row), Columns::from_row(&post_row)))
}

/// Build a remove Diff from a row delta.
pub fn build_remove_diff(catalog: &MaterializedCatalog, row_number: RowNumber, pre: EncodedRow) -> Option<Diff> {
	let row = decode_row(catalog, row_number, pre)?;
	Some(Diff::remove(Columns::from_row(&row)))
}

/// Build an insert Diff into a caller-provided `Columns` slab. The slab is
/// refilled in place via `Arc::make_mut`, then Arc-cloned into the resulting
/// `Diff`. Used by `CdcProducerActor` to reuse capacity across calls.
pub fn build_insert_diff_into(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	post: EncodedRow,
	post_buf: &mut Arc<Columns>,
) -> Option<Diff> {
	let row = decode_row(catalog, row_number, post)?;
	Arc::make_mut(post_buf).reset_from_row(&row);
	Some(Diff::insert_arc(post_buf.clone()))
}

/// Build an update Diff into caller-provided `Columns` slabs.
pub fn build_update_diff_into(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	pre: EncodedRow,
	post: EncodedRow,
	pre_buf: &mut Arc<Columns>,
	post_buf: &mut Arc<Columns>,
) -> Option<Diff> {
	let pre_row = decode_row(catalog, row_number, pre)?;
	let post_row = decode_row(catalog, row_number, post)?;
	Arc::make_mut(pre_buf).reset_from_row(&pre_row);
	Arc::make_mut(post_buf).reset_from_row(&post_row);
	Some(Diff::update_arc(pre_buf.clone(), post_buf.clone()))
}

/// Build a remove Diff into a caller-provided `Columns` slab.
pub fn build_remove_diff_into(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	pre: EncodedRow,
	pre_buf: &mut Arc<Columns>,
) -> Option<Diff> {
	let row = decode_row(catalog, row_number, pre)?;
	Arc::make_mut(pre_buf).reset_from_row(&row);
	Some(Diff::remove_arc(pre_buf.clone()))
}

/// Like `build_insert_diff_into` but sources inner `ColumnBuffer`s from a
/// shared `ColumnBufferPool` so per-row `Vec` allocations amortize across
/// deltas of any shape.
pub fn build_insert_diff_into_with_pool(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	post: EncodedRow,
	post_buf: &mut Arc<Columns>,
	pool: &ColumnBufferPool,
) -> Option<Diff> {
	let row = decode_row(catalog, row_number, post)?;
	Arc::make_mut(post_buf).reset_from_row_with_pool(&row, pool);
	Some(Diff::insert_arc(post_buf.clone()))
}

/// Like `build_update_diff_into` but sources inner `ColumnBuffer`s from a
/// shared `ColumnBufferPool`.
pub fn build_update_diff_into_with_pool(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	pre: EncodedRow,
	post: EncodedRow,
	pre_buf: &mut Arc<Columns>,
	post_buf: &mut Arc<Columns>,
	pool: &ColumnBufferPool,
) -> Option<Diff> {
	let pre_row = decode_row(catalog, row_number, pre)?;
	let post_row = decode_row(catalog, row_number, post)?;
	Arc::make_mut(pre_buf).reset_from_row_with_pool(&pre_row, pool);
	Arc::make_mut(post_buf).reset_from_row_with_pool(&post_row, pool);
	Some(Diff::update_arc(pre_buf.clone(), post_buf.clone()))
}

/// Like `build_remove_diff_into` but sources inner `ColumnBuffer`s from a
/// shared `ColumnBufferPool`.
pub fn build_remove_diff_into_with_pool(
	catalog: &MaterializedCatalog,
	row_number: RowNumber,
	pre: EncodedRow,
	pre_buf: &mut Arc<Columns>,
	pool: &ColumnBufferPool,
) -> Option<Diff> {
	let row = decode_row(catalog, row_number, pre)?;
	Arc::make_mut(pre_buf).reset_from_row_with_pool(&row, pool);
	Some(Diff::remove_arc(pre_buf.clone()))
}
