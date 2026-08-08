// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) binding {
		id: u64,
		namespace: u64,
		name: utf8,
		procedure_id: u64,
		protocol: utf8,
		http_method: utf8,
		http_path: utf8,
		rpc_name: utf8,
		format: utf8,
	}

	pub(crate) binding_namespace {
		id: u64,
		name: utf8,
	}
}
