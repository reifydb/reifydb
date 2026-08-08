// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) namespace {
		id: u64,
		name: utf8,
		parent_id: u64,
		grpc: utf8?,
		local_name: utf8?,
		token: utf8?,
	}
}
