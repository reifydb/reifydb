// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::interface::catalog::user::UserId;

pub type UserAuthenticationId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAuthenticationDef {
	pub id: UserAuthenticationId,
	pub user_id: UserId,
	pub method: String,
	pub properties: HashMap<String, String>,
}
