// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use reifydb_value::{Result, error};

use crate::error::diagnostic;

pub trait CdcConsume: Send + Sync + 'static {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>);

	fn overtaken(
		&self,
		cursor: CommitVersion,
		truncated_before: CommitVersion,
		reply: Box<dyn FnOnce(Result<CommitVersion>) + Send>,
	) {
		reply(Err(error!(diagnostic::consumer_overtaken("<unnamed>", cursor.0, truncated_before.0))))
	}
}

pub trait CdcConsumer {
	fn start(&mut self) -> Result<()>;
	fn stop(&mut self) -> Result<()>;
	fn is_running(&self) -> bool;
}
