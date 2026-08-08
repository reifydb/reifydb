// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) handler {
		id: u64,
		namespace: u64,
		name: utf8,
		on_sumtype_id: u64,
		on_variant_tag: u8,
		body_source: utf8,
	}

	pub(crate) handler_namespace {
		id: u64,
		name: utf8,
	}
}
