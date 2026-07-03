// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::row::{EncodedRow, SHAPE_HEADER_SIZE};
use reifydb_core::row::Row;
use reifydb_value::value::row_number::RowNumber;
use tracing::warn;

use crate::catalog::Catalog;

pub fn decode_row(catalog: &Catalog, row_number: RowNumber, row: EncodedRow) -> Option<Row> {
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
