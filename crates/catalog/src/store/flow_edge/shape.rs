// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) flow_edge {
		id: u64,
		flow: u64,
		source: u64,
		target: u64,
	}

	pub(crate) flow_edge_by_flow {
		flow: u64,
		id: u64,
	}
}
