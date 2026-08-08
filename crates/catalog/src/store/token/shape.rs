// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;
use reifydb_value::value::{datetime::DateTime, identity::IdentityId};

catalog_shape! {
	pub(crate) token {
		id: u64,
		token: utf8,
		identity: IdentityId,
		expires_at: DateTime?,
		created_at: DateTime,
	}
}
