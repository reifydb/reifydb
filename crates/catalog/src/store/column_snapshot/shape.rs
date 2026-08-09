// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) column_snapshot {
		id: u64,
		namespace: u64,
		kind: u8,
		source_id: u64,
		bucket_start: u64,
		bucket_width: u64,
		sequence_counter: u64,
		read_version: u64,
		row_count: u64,
	}
}
