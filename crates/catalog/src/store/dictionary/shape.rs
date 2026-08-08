// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) dictionary {
		id: u64,
		namespace: u64,
		name: utf8,
		value_type: u8,
		id_type: u8,
	}

	pub(crate) dictionary_namespace {
		id: u64,
		name: utf8,
	}
}
