// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) column_property {
		id: u64,
		column: u64,
		policy: u8,
		value: u8,
	}
}
