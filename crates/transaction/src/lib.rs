// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{
	fmt::{Display, Formatter},
	ops::Deref,
};

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	return_internal_error,
};
use reifydb_type::{error::Error, value::uuid::Uuid7};

pub mod change;
pub mod delta;
pub mod interceptor;
pub mod multi;
pub mod single;
pub mod standard;

/// A unique identifier for a transaction using UUIDv7 for time-ordered
/// uniqueness
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId(pub(crate) Uuid7);

impl Default for TransactionId {
	fn default() -> Self {
		Self::generate()
	}
}

impl Deref for TransactionId {
	type Target = Uuid7;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TransactionId {
	pub fn generate() -> Self {
		Self(Uuid7::generate())
	}
}

impl TryFrom<&[u8]> for TransactionId {
	type Error = Error;

	fn try_from(bytes: &[u8]) -> std::result::Result<Self, Self::Error> {
		if bytes.len() != 16 {
			return_internal_error!("Invalid transaction ID length: expected 16 bytes, got {}", bytes.len());
		}
		let mut uuid_bytes = [0u8; 16];
		uuid_bytes.copy_from_slice(bytes);
		let uuid = uuid::Uuid::from_bytes(uuid_bytes);
		Ok(Self(Uuid7::from(uuid)))
	}
}

impl Display for TransactionId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

pub struct TransactionVersion;

impl HasVersion for TransactionVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "transaction".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Transaction management and concurrency control module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
