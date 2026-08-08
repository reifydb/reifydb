// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) operator {
		id: u64,
		flow: u64,
		r#type: u8,
		data: blob,
	}

	pub(crate) operator_by_flow {
		flow: u64,
		id: u64,
	}
}
