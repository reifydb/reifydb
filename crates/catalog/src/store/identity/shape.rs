// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::catalog_shape;
use reifydb_value::value::identity::{IdentityId, IdentityKind};

catalog_shape! {
	pub(crate) identity {
		identity: IdentityId,
		name: utf8,
		enabled: bool,
		kind: IdentityKind,
	}
}
