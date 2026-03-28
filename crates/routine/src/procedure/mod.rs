// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod context;
pub mod error;
pub mod identity;
pub mod registry;

pub mod clock;
pub mod set;

use error::ProcedureError;
use registry::{Procedures, ProceduresBuilder};
use reifydb_core::value::column::columns::Columns;
use reifydb_transaction::transaction::Transaction;

/// A server-side procedure that can mutate database state within a transaction.
pub trait Procedure: Send + Sync {
	fn call(&self, ctx: &context::ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError>;
}

pub fn default_procedures() -> ProceduresBuilder {
	Procedures::builder()
		.with_procedure("system::config::set", set::config::SetConfigProcedure::new)
		.with_procedure("clock::set", clock::set::ClockSetProcedure::new)
		.with_procedure("clock::advance", clock::advance::ClockAdvanceProcedure::new)
		.with_procedure("identity::inject", identity::inject::IdentityInject::new)
}
