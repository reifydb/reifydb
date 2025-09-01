// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Identity, Transaction};
use reifydb_engine::StandardEngine;

pub struct CommandSession<T: Transaction> {
	pub(crate) engine: StandardEngine<T>,
	pub(crate) identity: Identity,
}
