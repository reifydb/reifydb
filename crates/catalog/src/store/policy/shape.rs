// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) policy {
		id: u64,
		name: utf8,
		target_type: utf8,
		target_namespace: utf8,
		target_object: utf8,
		enabled: bool,
	}

	pub(crate) policy_op {
		policy_id: u64,
		operation: utf8,
		body_source: utf8,
	}
}
