// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_engine::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

/// Cancels a pipeline run: skips all pending/blocked job_runs and pending step_runs.
///
/// Expects 1 positional argument: run_id (Uuid4).
pub struct CancelRunProcedure;

impl Procedure for CancelRunProcedure {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let run_id = match ctx.params {
			Params::Positional(args) if !args.is_empty() => args[0].clone(),
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::cancel_run"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::cancel_run"),
					expected: 1,
					actual: 0,
				});
			}
		};

		let run_id_str = match &run_id {
			Value::Uuid4(u) => u.to_string(),
			Value::Utf8(s) => s.clone(),
			_ => {
				return Err(ProcedureError::InvalidArgumentType {
					procedure: Fragment::internal("forge::cancel_run"),
					argument_index: 0,
					expected: vec![Type::Uuid4, Type::Utf8],
					actual: run_id.get_type(),
				});
			}
		};

		// Update run status to cancelled
		tx.rql(
			&format!("UPDATE forge::runs {{ status: \"cancelled\", finished_at: datetime::now() }} \
				 FILTER id == uuid::v4(\"{run_id_str}\")"),
			Params::None,
		)?;

		// Skip all pending and blocked job_runs
		tx.rql(
			&format!("UPDATE forge::job_runs {{ status: \"skipped\" }} \
				 FILTER run_id == uuid::v4(\"{run_id_str}\") AND status == \"pending\""),
			Params::None,
		)?;

		tx.rql(
			&format!("UPDATE forge::job_runs {{ status: \"skipped\" }} \
				 FILTER run_id == uuid::v4(\"{run_id_str}\") AND status == \"blocked\""),
			Params::None,
		)?;

		// Skip all pending step_runs
		tx.rql(
			&format!("UPDATE forge::step_runs {{ status: \"skipped\" }} \
				 FILTER run_id == uuid::v4(\"{run_id_str}\") AND status == \"pending\""),
			Params::None,
		)?;

		Ok(Columns::single_row([
			("run_id", Value::Utf8(run_id_str)),
			("status", Value::Utf8("cancelled".to_string())),
		]))
	}
}
