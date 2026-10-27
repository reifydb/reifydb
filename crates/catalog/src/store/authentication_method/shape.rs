// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;
use reifydb_value::value::duration::Duration;

catalog_shape! {
	pub(crate) authentication_method {
		id: u64,
		name: utf8,
		ttl: Duration?,
		promote_guest: bool,
		properties: utf8,
	}
}
