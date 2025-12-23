// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::interface::Cdc;
use reifydb_engine::StandardCommandTransaction;
use reifydb_type::Result;

/// Trait for CDC transaction processing functions
#[async_trait]
pub trait CdcConsume: Send + Sync + 'static {
	async fn consume(&self, txn: &mut StandardCommandTransaction, transactions: Vec<Cdc>) -> Result<()>;
}

/// Trait for CDC event stream consumers
pub trait CdcConsumer: Send + Sync {
	fn start(&mut self) -> Result<()>;
	fn stop(&mut self) -> Result<()>;
	fn is_running(&self) -> bool;
}
