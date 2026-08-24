// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::policy::{CallableOp, PolicyTargetType},
	value::column::columns::Columns,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_policy::error::PolicyError;
use reifydb_routine_abi::{Procedure as RoutineProcedure, context::ProcedureContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, params::Params};

use crate::{Result, policy::PolicyEvaluator, vm::services::Services};

pub(crate) enum CallSite<'a> {
	Named,
	EventHandler {
		event: &'a str,
		variant: &'a str,
	},
}

pub(crate) struct ProcedureCall<'a> {
	pub routine: &'a Arc<dyn RoutineProcedure>,
	pub fragment: &'a Fragment,
	pub target: &'a str,
	pub params: &'a Params,
}

pub(crate) fn invoke_procedure_routine(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	tx: &mut Transaction<'_>,
	call: ProcedureCall<'_>,
	site: CallSite<'_>,
) -> Result<Columns> {
	enforce_call_policy(services, symbols, tx, call.target, site)?;

	let identity = tx.identity();
	let mut ctx = ProcedureContext {
		fragment: call.fragment.clone(),
		identity,
		row_count: 1,
		runtime_context: &services.runtime_context,
		tx,
		params: call.params,
		catalog: &services.catalog,
		ioc: &services.ioc,
	};
	let empty = Columns::empty();
	call.routine.call(&mut ctx, &empty).map_err(|e| e.with_context(call.fragment.clone(), true))
}

pub(crate) fn enforce_call_policy(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	tx: &mut Transaction<'_>,
	target: &str,
	site: CallSite<'_>,
) -> Result<()> {
	let (namespace, object) = match Catalog::split_qualified_name(target) {
		Some((namespace, object)) => (namespace, object.to_string()),
		None => ("default".to_string(), target.to_string()),
	};
	PolicyEvaluator::new(services, symbols)
		.enforce_identity_policy(tx, &namespace, &object, CallableOp::Call, PolicyTargetType::Procedure)
		.map_err(|denial| match site {
			CallSite::Named => denial,
			CallSite::EventHandler {
				event,
				variant,
			} => PolicyError::HandlerCallDenied {
				event: event.to_string(),
				variant: variant.to_string(),
				handler: target.to_string(),
			}
			.into(),
		})
}
