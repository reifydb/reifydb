// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Decode row deltas into columnar `Diff` objects.
//!
//! Uses fingerprint-based shape lookup from `RowShapeRegistry` to convert
//! `EncodedRow` into `Row`s and then into `Columns`.

use reifydb_core::{
	encoded::row::{EncodedRow, SCHEMA_HEADER_SIZE},
	interface::change::Diff,
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::value::row_number::RowNumber;
use tracing::warn;

use super::RowShapeRegistry;

/// Try to decode an EncodedRow into a Row using the shape registry.
/// Returns None if the values are too short or the shape is not in the cache.
fn decode_row(registry: &RowShapeRegistry, row_number: RowNumber, row: EncodedRow) -> Option<Row> {
	if row.len() < SCHEMA_HEADER_SIZE {
		warn!("EncodedRow too short for shape fingerprint ({} < {})", row.len(), SCHEMA_HEADER_SIZE);
		return None;
	}
	let fingerprint = row.fingerprint();
	let shape = registry.get(fingerprint);
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
pub fn build_insert_diff(registry: &RowShapeRegistry, row_number: RowNumber, post: EncodedRow) -> Option<Diff> {
	let row = decode_row(registry, row_number, post)?;
	let columns = Columns::from_row(&row);
	Some(Diff::Insert {
		post: columns,
	})
}

/// Build an update Diff from a row delta with pre and post values.
pub fn build_update_diff(
	registry: &RowShapeRegistry,
	row_number: RowNumber,
	pre: EncodedRow,
	post: EncodedRow,
) -> Option<Diff> {
	let pre_row = decode_row(registry, row_number, pre)?;
	let post_row = decode_row(registry, row_number, post)?;
	let pre_cols = Columns::from_row(&pre_row);
	let post_cols = Columns::from_row(&post_row);
	Some(Diff::Update {
		pre: pre_cols,
		post: post_cols,
	})
}

/// Build a remove Diff from a row delta.
pub fn build_remove_diff(registry: &RowShapeRegistry, row_number: RowNumber, pre: EncodedRow) -> Option<Diff> {
	let row = decode_row(registry, row_number, pre)?;
	let columns = Columns::from_row(&row);
	Some(Diff::Remove {
		pre: columns,
	})
}
