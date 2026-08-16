// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;
use reifydb_value::value::duration::Duration;

catalog_shape! {
	pub(crate) row_settings {
		duration: Duration,
		persistent: u8,
	}
}
