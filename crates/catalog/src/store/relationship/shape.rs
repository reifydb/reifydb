// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) relationship {
		id: u64,
		namespace_id: u64,
		name: utf8,
		source_table_id: u64,
		source_column_id: u64,
		target_table_id: u64,
		target_column_id: u64,
		junction_table_id: u64,
		junction_source_column_id: u64,
		junction_target_column_id: u64,
		cardinality: u8,
	}
}
