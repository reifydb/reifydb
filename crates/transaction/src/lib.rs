// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use reifydb_type::Result;

pub mod mvcc;
pub mod svl;

use reifydb_core::interface::version::{
	ComponentKind, HasVersion, SystemVersion,
};

pub struct TransactionVersion;

impl HasVersion for TransactionVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
            name: "transaction".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: "Transaction management and concurrency control module".to_string(),
            kind: ComponentKind::Module,
        }
	}
}
