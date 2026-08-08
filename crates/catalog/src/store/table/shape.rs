// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) table {
		id: u64,
		namespace: u64,
		name: utf8,
		primary_key: u64,
		partition_by: utf8,
		underlying: u8,
		ts: utf8,
		time_domain: u8,
	}

	pub(crate) table_namespace {
		id: u64,
		name: utf8,
	}
}
