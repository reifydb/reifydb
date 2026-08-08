// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;
use reifydb_value::value::identity::IdentityId;

catalog_shape! {
	pub(crate) granted_role {
		identity: IdentityId,
		role_id: u64,
	}
}
