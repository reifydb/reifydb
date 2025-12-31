// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod cdc;
pub mod interceptor;
pub mod multi;
pub mod single;
pub mod standard;

pub use interceptor::WithInterceptors;
pub use reifydb_store_transaction::{ObjectId, StorageStats, StorageTracker, Tier, TierStats, TransactionStore};
pub use reifydb_type::Result;
pub use standard::{
	IntoStandardTransaction, StandardCommandTransaction, StandardQueryTransaction, StandardTransaction,
};

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
