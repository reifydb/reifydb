// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) flow {
		id: u64,
		namespace: u64,
		name: utf8,
		status: u8,
	}

	pub(crate) flow_namespace {
		id: u64,
		name: utf8,
	}
}
