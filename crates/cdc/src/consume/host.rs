// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::common::CommitVersion;
use reifydb_transaction::transaction::{command::CommandTransaction, query::QueryTransaction};
use reifydb_type::Result;

pub trait CdcHost: Clone + Send + Sync + 'static {
	/// Begin a new command transaction.
	fn begin_command(&self) -> Result<CommandTransaction>;

	/// Begin a new read-only query transaction.
	fn begin_query(&self) -> Result<QueryTransaction>;

	/// Get the current committed version.
	fn current_version(&self) -> Result<CommitVersion>;

	/// Get the version up to which all transactions are complete.
	fn done_until(&self) -> CommitVersion;
}
