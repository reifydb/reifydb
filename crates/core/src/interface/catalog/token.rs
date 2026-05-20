// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{datetime::DateTime, identity::IdentityId};
use serde::{Deserialize, Serialize};

pub type TokenId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Token {
	pub id: TokenId,
	pub token: String,
	pub identity: IdentityId,
	pub expires_at: Option<DateTime>,
	pub created_at: DateTime,
}
