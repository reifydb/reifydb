// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{datetime::DateTime, identity::IdentityId};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::user::UserId;

pub type TokenId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenDef {
	pub id: TokenId,
	pub token: String,
	pub identity: IdentityId,
	pub user: UserId,
	pub expires_at: Option<DateTime>,
	pub created_at: DateTime,
}
