// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod cdc;
pub mod change;
pub mod interceptor;
mod transaction;
mod unversioned;
mod versioned;

use crate::value::uuid::Uuid7;
pub use cdc::{
	CdcQueryTransaction, CdcTransaction,
};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
pub use transaction::{CommandTransaction, QueryTransaction};
pub use unversioned::*;
pub use versioned::*;

/// A unique identifier for a transaction using UUIDv7 for time-ordered uniqueness
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId(pub(crate) Uuid7);

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

impl Display for TransactionId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

pub trait Transaction: Send + Sync + Clone + 'static {
	type Versioned: VersionedTransaction;
	type Unversioned: UnversionedTransaction;
	type Cdc: CdcTransaction;
}
