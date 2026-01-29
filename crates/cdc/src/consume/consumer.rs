// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::cdc::Cdc;
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_type::Result;

/// Trait for CDC transaction processing functions
pub trait CdcConsume {
	fn consume(&self, txn: &mut CommandTransaction, transactions: Vec<Cdc>) -> Result<()>;
}

/// Trait for CDC event stream consumers
pub trait CdcConsumer {
	fn start(&mut self) -> Result<()>;
	fn stop(&mut self) -> Result<()>;
	fn is_running(&self) -> bool;
}
