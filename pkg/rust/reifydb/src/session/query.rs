// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::Identity;
use reifydb_engine::StandardEngine;

/// Session for executing read-only database queries
pub struct QuerySession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: Identity,
}
