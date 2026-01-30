// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::auth::Identity;
use reifydb_engine::engine::StandardEngine;

pub struct AdminSession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: Identity,
}
