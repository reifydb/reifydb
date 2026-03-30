// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use std::{
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	return_internal_error,
};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_type::{error::Error, value::uuid::Uuid7};
use uuid::{Builder, Uuid};

pub mod change;
pub mod change_accumulator;
pub mod delta;
pub mod error;
pub mod interceptor;
pub mod multi;
pub mod single;
pub mod transaction;

/// A unique identifier for a transaction using UUIDv7 for time-ordered
/// uniqueness
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TransactionId(pub(crate) Uuid7);

impl Deref for TransactionId {
	type Target = Uuid7;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TransactionId {
	/// Generate a new transaction ID using the infrastructure RNG stream.
	///
	/// Uses `rng.infra_bytes_10()` instead of `rng.bytes_10()` so that
	/// transaction ID generation does not consume from the primary RNG.
	/// This ensures deterministic test output across runners that create
	/// different numbers of internal transactions (e.g. gRPC vs embedded).
	pub fn generate(clock: &Clock, rng: &Rng) -> Self {
		let millis = clock.now_millis();
		let random_bytes = rng.infra_bytes_10();
		Self(Uuid7(Builder::from_unix_timestamp_millis(millis, &random_bytes).into_uuid()))
	}
}

impl TryFrom<&[u8]> for TransactionId {
	type Error = Error;

	fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
		if bytes.len() != 16 {
			return_internal_error!("Invalid transaction ID length: expected 16 bytes, got {}", bytes.len());
		}
		let mut uuid_bytes = [0u8; 16];
		uuid_bytes.copy_from_slice(bytes);
		let uuid = Uuid::from_bytes(uuid_bytes);
		Ok(Self(Uuid7::from(uuid)))
	}
}

impl Display for TransactionId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

pub struct TransactionVersion;

impl HasVersion for TransactionVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Transaction management and concurrency control module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
