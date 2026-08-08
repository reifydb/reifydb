// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) sink {
		id: u64,
		namespace: u64,
		name: utf8,
		source_namespace: u64,
		source_name: utf8,
		connector: utf8,
		config: utf8,
		status: u8,
	}

	pub(crate) sink_namespace {
		id: u64,
		name: utf8,
	}
}
