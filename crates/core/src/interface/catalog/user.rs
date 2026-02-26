// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

pub type UserId = u64;
pub type RoleId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserDef {
	pub id: UserId,
	pub name: String,
	pub password_hash: String,
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
