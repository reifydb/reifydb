// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::bytes::{EncodedBytes, SHAPE_HEADER_SIZE};
use reifydb_core::row::Row;
use reifydb_value::value::row_number::RowNumber;
use tracing::warn;

use crate::catalog::Catalog;

pub fn decode_bytes(catalog: &Catalog, row_number: RowNumber, bytes: EncodedBytes) -> Option<Row> {
	if bytes.len() < SHAPE_HEADER_SIZE {
		warn!("EncodedBytes too short for shape fingerprint ({} < {})", bytes.len(), SHAPE_HEADER_SIZE);
		return None;
	}
	let fingerprint = bytes.fingerprint();
	let shape = catalog.find_row_shape(fingerprint);
	match shape {
		Some(shape) => Some(Row {
			number: row_number,
			encoded: bytes,
			shape,
		}),
		None => {
			warn!(?fingerprint, "RowShape not found in cache for row decode");
			None
		}
	}
}
