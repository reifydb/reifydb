// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_value::value::identity::IdentityId;
use serde::{Deserialize, Serialize};

pub type AuthenticationId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Authentication {
	pub id: AuthenticationId,
	pub identity: IdentityId,
	pub method: String,
	pub properties: HashMap<String, String>,
}
