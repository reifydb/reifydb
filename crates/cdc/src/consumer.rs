// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::Cdc;
use reifydb_engine::StandardCommandTransaction;
use reifydb_type::Result;

/// Trait for CDC transaction processing functions
pub trait CdcConsume {
	fn consume(&self, txn: &mut StandardCommandTransaction, transactions: Vec<Cdc>) -> Result<()>;
}

/// Trait for CDC event stream consumers
pub trait CdcConsumer {
	fn start(&mut self) -> Result<()>;
	fn stop(&mut self) -> Result<()>;
	fn is_running(&self) -> bool;
}
