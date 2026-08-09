// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) series {
		id: u64,
		namespace: u64,
		name: utf8,
		tag: u64,
		key_column: utf8,
		key_kind: u8,
		precision: u8,
		primary_key: u64,
		partition_by: utf8,
		underlying: u8,
		ts: utf8,
		time_domain: u8,
	}

	pub(crate) series_namespace {
		id: u64,
		name: utf8,
	}
}
