// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns};
use reifydb_rql::{compiler::CompilationResult, instruction::ScopeType, nodes::DispatchNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use crate::{
	expression::{context::EvalContext, eval::evaluate},
	vm::{services::Services, stack::Variable, vm::Vm},
};

pub(crate) const MAX_DISPATCH_DEPTH: u8 = 32;

pub(crate) fn dispatch(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: DispatchNode,
	params: &Params,
	dispatch_depth: u8,
) -> crate::Result<Columns> {
	if dispatch_depth >= MAX_DISPATCH_DEPTH {
		return Err(reifydb_core::internal_error!(
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
		return Err(reifydb_core::internal_error!(
			"Variant '{}' not found in event type '{}'",
			plan.variant_name,
			sumtype_def.name
		));
	};
	let variant_tag = variant_def.tag;

	// List all handlers for this event variant
	let handlers = {
		let mut tx_tmp = tx.reborrow();
		services.catalog.list_handlers_for_variant(&mut tx_tmp, plan.on_sumtype_id, variant_tag)?
	};

	let handler_count = handlers.len();

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
		};
		let col = evaluate(&eval_ctx, expr, &services.functions, &services.clock)?;
		event_columns.push(Column::new(Fragment::internal(field_name), col.data));
	}
	let event_payload = Columns::new(event_columns);

	// Fire each handler in declaration order
	for handler in handlers {
		let compiled = services.compiler.compile(tx, &handler.body_source)?;

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
				return Err(reifydb_core::internal_error!(
					"Handler body requires more input during dispatch"
				));
			}
		}
	}

	Ok(Columns::single_row([("handlers_fired", Value::Uint1(handler_count as u8))]))
}
