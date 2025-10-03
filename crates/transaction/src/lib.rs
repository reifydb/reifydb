// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod multi;
pub mod single;

pub use reifydb_type::Result;

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
