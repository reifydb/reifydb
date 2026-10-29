// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_transaction::transaction::{command::CommandTransaction, query::QueryTransaction};
use reifydb_value::Result;

pub trait CdcHost: Clone + Send + Sync + 'static {
	fn begin_command(&self) -> Result<CommandTransaction>;

	fn begin_query(&self) -> Result<QueryTransaction>;

	fn catalog(&self) -> &Catalog;
}
