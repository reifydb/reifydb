// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod cdc;
mod change;
pub mod interceptor;
mod multi;
mod single;
mod transaction;

use std::{
	fmt::{Display, Formatter},
	ops::Deref,
};

pub use cdc::{CdcQueryTransaction, CdcTransaction};
pub use change::*;
pub use multi::*;
use reifydb_type::{Error, Uuid7, return_internal_error};
pub use single::*;
pub use transaction::{CommandTransaction, QueryTransaction};

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

pub trait Transaction: Send + Sync + Clone + 'static {
	type MultiVersion: MultiVersionTransaction;
	type SingleVersion: SingleVersionTransaction;
	type Cdc: CdcTransaction;
}
