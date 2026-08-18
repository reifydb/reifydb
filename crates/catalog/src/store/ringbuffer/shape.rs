// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) ringbuffer {
		id: u64,
		namespace: u64,
		name: utf8,
		capacity: u64,
		primary_key: u64,
		partition_by: utf8,
		ts: utf8,
		time_domain: u8,
	}

	pub(crate) ringbuffer_namespace {
		id: u64,
		name: utf8,
	}
}
