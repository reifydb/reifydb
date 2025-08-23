// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod cdc;
pub mod interceptor;
mod transaction;
mod unversioned;
mod versioned;

pub use cdc::{
	CdcQueryTransaction, CdcTransaction, StandardCdcQueryTransaction,
	StandardCdcTransaction,
};
pub use transaction::{CommandTransaction, QueryTransaction};
pub use unversioned::*;
pub use versioned::*;

pub trait Transaction: Send + Sync + Clone + 'static {
	type Versioned: VersionedTransaction;
	type Unversioned: UnversionedTransaction;
	type Cdc: CdcTransaction;
}
