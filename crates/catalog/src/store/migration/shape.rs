// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) migration {
		id: u64,
		name: utf8,
		body: utf8,
		rollback_body: utf8,
		hash: u128,
	}

	pub(crate) migration_event {
		id: u64,
		migration_id: u64,
		action: u8,
	}
}
