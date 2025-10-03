// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Identity;
use reifydb_engine::StandardEngine;

pub struct CommandSession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: Identity,
}
