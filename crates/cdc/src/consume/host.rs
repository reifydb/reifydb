// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::CommitVersion;
use reifydb_transaction::transaction::{command::CommandTransaction, query::QueryTransaction};
use reifydb_type::Result;

pub trait CdcHost: Clone + Send + Sync + 'static {
	fn begin_command(&self) -> Result<CommandTransaction>;

	fn begin_query(&self) -> Result<QueryTransaction>;

	fn current_version(&self) -> Result<CommitVersion>;

	fn done_until(&self) -> CommitVersion;

	fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool;

	fn catalog(&self) -> &Catalog;
}
