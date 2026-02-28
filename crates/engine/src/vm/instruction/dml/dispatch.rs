// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	internal_error,
	value::column::{Column, columns::Columns},
};
use reifydb_rql::{compiler::CompilationResult, instruction::ScopeType, nodes::DispatchNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId},
};

use crate::{
	Result,
	expression::{context::EvalContext, eval::evaluate},
	procedure::context::ProcedureContext,
	vm::{executor::Executor, services::Services, stack::Variable, vm::Vm},
};

pub(crate) const MAX_DISPATCH_DEPTH: u8 = 32;

pub(crate) fn dispatch(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: DispatchNode,
	params: &Params,
	dispatch_depth: u8,
) -> Result<Columns> {
	if dispatch_depth >= MAX_DISPATCH_DEPTH {
		return Err(internal_error!(
			"Max dispatch depth ({}) exceeded for event variant '{}'",
			MAX_DISPATCH_DEPTH,
			plan.variant_name
		));
	}

	// Find the variant in the sumtype to get the tag
	let sumtype_def = {
		let mut tx_tmp = tx.reborrow();
		services.catalog.get_sumtype(&mut tx_tmp, plan.on_sumtype_id)?
	};

	let variant_name_lower = plan.variant_name.to_lowercase();
	let Some(variant_def) = sumtype_def.variants.iter().find(|v| v.name == variant_name_lower) else {
		return Err(internal_error!(
			"Variant '{}' not found in event type '{}'",
			plan.variant_name,
			sumtype_def.name
		));
	};
	let variant_tag = variant_def.tag;

	// List all procedures with event binding for this variant
	let procedures = {
		let mut tx_tmp = tx.reborrow();
		services.catalog.list_procedures_for_variant(&mut tx_tmp, plan.on_sumtype_id, variant_tag)?
	};

	let handler_count = procedures.len();

	// Evaluate dispatch fields into a Columns payload
	let mut event_columns = Vec::with_capacity(plan.fields.len());
	for (field_name, expr) in &plan.fields {
		let eval_ctx = EvalContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params,
			symbol_table: &vm.symbol_table,
			is_aggregate_context: false,
			functions: &services.functions,
			clock: &services.clock,
			arena: None,
			identity: IdentityId::anonymous(),
		};
		let col = evaluate(&eval_ctx, expr, &services.functions, &services.clock)?;
		event_columns.push(Column::new(Fragment::internal(field_name), col.data));
	}
	let event_payload = Columns::new(event_columns);

	// Fire each catalog (RQL) procedure in declaration order
	for procedure in &procedures {
		let compiled = services.compiler.compile(tx, &procedure.body)?;

		match compiled {
			CompilationResult::Ready(compiled_list) => {
				let saved_ip = vm.ip;

				// Enter handler scope
				vm.symbol_table.enter_scope(ScopeType::Function);
				for col in event_payload.columns.iter() {
					let var_name = format!("event_{}", col.name.text());
					let scalar = Columns::new(vec![col.clone()]);
					vm.symbol_table.set(var_name, Variable::Scalar(scalar), true)?;
				}

				let mut handler_result = Vec::new();
				for compiled_unit in compiled_list.iter() {
					vm.ip = 0;
					vm.run(services, tx, &compiled_unit.instructions, params, &mut handler_result)?;
				}

				vm.ip = saved_ip;
				let _ = vm.symbol_table.exit_scope();
			}
			CompilationResult::Incremental(_) => {
				return Err(internal_error!("Handler body requires more input during dispatch"));
			}
		}
	}

	// Fire native (runtime-registered) handlers
	let native_handlers = services.procedures.get_handlers(plan.on_sumtype_id, variant_tag);
	let native_count = native_handlers.len();
	if !native_handlers.is_empty() {
		// Build named params from event payload (single-row columns → scalar values)
		let mut named_map = HashMap::new();
		for col in event_payload.columns.iter() {
			let key = format!("event_{}", col.name.text());
			if let Some(val) = col.data.iter().next() {
				named_map.insert(key, val);
			}
		}
		let call_params = Params::Named(named_map);
		let identity = IdentityId::anonymous();
		let executor = Executor::from_services(services.clone());

		for native_proc in native_handlers {
			let ctx = ProcedureContext {
				identity,
				params: &call_params,
				catalog: &services.catalog,
				functions: &services.functions,
				clock: &services.clock,
				executor: &executor,
			};
			let _result = native_proc.call(&ctx, tx)?;
		}
	}

	let total_fired = handler_count + native_count;
	Ok(Columns::single_row([("handlers_fired", Value::Uint1(total_fired as u8))]))
}
