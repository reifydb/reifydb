// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Result,
	interface::{ActiveCommandTransaction, CdcEvent, Transaction},
};

/// Trait for CDC event processing functions
pub trait CdcConsume<T: Transaction>: Send + Sync + 'static {
	fn consume(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		events: Vec<CdcEvent>,
	) -> Result<()>;
}

/// Trait for CDC event stream consumers
pub trait CdcConsumer: Send + Sync {
	/// Starts the consumer
	fn start(&mut self) -> Result<()>;

	/// Stops the consumer
	fn stop(&mut self) -> Result<()>;

	/// Returns whether the consumer is running
	fn is_running(&self) -> bool;
}
