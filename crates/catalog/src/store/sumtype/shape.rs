// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) sumtype {
		id: u64,
		namespace: u64,
		name: utf8,
		variants_json: utf8,
		kind: u8,
	}

	pub(crate) sumtype_namespace {
		id: u64,
		name: utf8,
	}
}
