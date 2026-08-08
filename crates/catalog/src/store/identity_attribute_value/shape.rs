// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;
use reifydb_value::value::identity::IdentityId;

catalog_shape! {
	pub(crate) identity_attribute_value {
		identity: IdentityId,
		attribute: u64,
		value: blob,
	}
}
