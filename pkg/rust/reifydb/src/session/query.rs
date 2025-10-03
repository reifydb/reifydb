// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Identity;
use reifydb_engine::StandardEngine;

/// Session for executing read-only database queries
pub struct QuerySession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: Identity,
}
