// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::identity::IdentityId;
use serde::{Deserialize, Serialize};

pub type RoleId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
	pub id: IdentityId,
	pub name: String,
	pub enabled: bool,
}

impl Identity {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Role {
	pub id: RoleId,
	pub name: String,
}

impl Role {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GrantedRole {
	pub identity: IdentityId,
	pub role_id: RoleId,
}
