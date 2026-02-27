// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_engine::engine::StandardEngine;
use reifydb_type::value::identity::IdentityId;

/// Session for executing read-only database queries
pub struct QuerySession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: IdentityId,
}
