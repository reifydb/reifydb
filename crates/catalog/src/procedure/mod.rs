// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod context;
pub mod error;
pub mod registry;

use error::ProcedureError;
use reifydb_core::value::column::columns::Columns;
use reifydb_transaction::transaction::Transaction;

/// A server-side procedure that can mutate database state within a transaction.
pub trait Procedure: Send + Sync {
	fn call(&self, ctx: &context::ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError>;
}
