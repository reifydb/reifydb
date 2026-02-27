// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::identity::IdentityId;
use serde::{Deserialize, Serialize};

pub type UserId = u64;
pub type RoleId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserDef {
	pub id: UserId,
	pub identity: IdentityId,
	pub name: String,
	pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoleDef {
	pub id: RoleId,
	pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserRoleDef {
	pub user_id: UserId,
	pub role_id: RoleId,
}
