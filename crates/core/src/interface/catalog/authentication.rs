// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_type::value::identity::IdentityId;
use serde::{Deserialize, Serialize};

pub type AuthenticationId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthenticationDef {
	pub id: AuthenticationId,
	pub identity: IdentityId,
	pub method: String,
	pub properties: HashMap<String, String>,
}
