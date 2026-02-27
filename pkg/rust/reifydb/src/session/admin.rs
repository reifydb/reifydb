// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_engine::engine::StandardEngine;
use reifydb_type::value::identity::IdentityId;

pub struct AdminSession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: IdentityId,
}
