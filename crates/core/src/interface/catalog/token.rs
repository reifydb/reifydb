// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	encoding::{LeBytes, RowField},
	value::{datetime::DateTime, identity::IdentityId, value_type::ValueType},
};
use serde::{Deserialize, Serialize};

pub type TokenId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenKind {
	Session,
	ApiKey,
}

impl LeBytes for TokenKind {
	type Bytes = [u8; 1];

	#[inline]
	fn to_le_bytes(&self) -> Self::Bytes {
		match self {
			TokenKind::Session => [0],
			TokenKind::ApiKey => [1],
		}
	}

	#[inline]
	fn from_le_bytes(bytes: Self::Bytes) -> Self {
		match bytes[0] {
			0 => TokenKind::Session,
			1 => TokenKind::ApiKey,
			other => panic!("token row carries unknown token kind {other}"),
		}
	}
}

impl RowField for TokenKind {
	const VALUE_TYPE: ValueType = ValueType::Uint1;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Token {
	pub id: TokenId,
	pub token: String,
	pub identity: IdentityId,
	pub expires_at: Option<DateTime>,
	pub created_at: DateTime,
	pub kind: TokenKind,
}
