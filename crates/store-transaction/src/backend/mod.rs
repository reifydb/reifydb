// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::backend::{multi::BackendMulti, single::BackendSingle};

pub(crate) mod commit;
pub(crate) mod diagnostic;
pub mod memory;
pub mod multi;
pub mod single;
pub mod sqlite;

#[derive(Clone)]
pub struct Backend {
	pub multi: BackendMulti,
	pub single: BackendSingle,
}
