// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) column {
		id: u64,
		object: u64,
		name: utf8,
		value: u8,
		index: u8,
		auto_increment: bool,
		constraint: blob,
		dictionary_id: u64,
	}

	pub(crate) object_column {
		id: u64,
		name: utf8,
		index: u8,
	}
}
