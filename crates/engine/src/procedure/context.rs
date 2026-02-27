// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_function::registry::Functions;
use reifydb_runtime::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::vm::executor::Executor;

pub struct ProcedureContext<'a> {
	pub identity: IdentityId,
	pub params: &'a Params,
	pub catalog: &'a Catalog,
	pub functions: &'a Functions,
	pub clock: &'a Clock,
	pub executor: &'a Executor,
}

impl ProcedureContext<'_> {
	/// Execute RQL within the current transaction.
	pub fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> crate::Result<Vec<Frame>> {
		self.executor.rql(tx, self.identity, rql, params)
	}
}
