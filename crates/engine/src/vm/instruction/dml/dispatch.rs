// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	internal_error,
	testing::CapturedInvocation,
	value::column::{Column, columns::Columns},
};
use reifydb_routine::procedure::context::ProcedureContext;
use reifydb_rql::{compiler::CompilationResult, instruction::ScopeType, nodes::DispatchNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, sumtype::VariantRef},
};

use crate::{
	Result,
	expression::{context::EvalSession, eval::evaluate},
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
) -> Result<Columns> {
	if dispatch_depth >= MAX_DISPATCH_DEPTH {
		return Err(internal_error!(
			"Max dispatch depth ({}) exceeded for event variant '{}'",
			MAX_DISPATCH_DEPTH,
			plan.variant_name
		));
	}

	// Find the variant in the sumtype to get the tag
	let sumtype = {
		let mut tx_tmp = tx.reborrow();
		services.catalog.get_sumtype(&mut tx_tmp, plan.on_sumtype_id)?
	};

	let variant_name_lower = plan.variant_name.to_lowercase();
	let Some(variant) = sumtype.variants.iter().find(|v| v.name == variant_name_lower) else {
		return Err(internal_error!(
			"Variant '{}' not found in event type '{}'",
			plan.variant_name,
			sumtype.name
		));
	};
	let variant_tag = variant.tag;

	let variant_ref = VariantRef {
		sumtype_id: plan.on_sumtype_id,
		variant_tag,
	};

	// List all procedures with event binding for this variant
	let procedures = {
		let mut tx_tmp = tx.reborrow();
		services.catalog.list_procedures_for_variant(&mut tx_tmp, variant_ref)?
	};

	let handler_count = procedures.len();

	// Evaluate dispatch fields into a Columns payload
	let session = EvalSession {
		params,
		symbols: &vm.symbols,
		functions: &services.functions,
		runtime_context: &services.runtime_context,
		arena: None,
		identity: tx.identity(),
		is_aggregate_context: false,
	};
	let mut event_columns = Vec::with_capacity(plan.fields.len());
	for (field_name, expr) in &plan.fields {
		let eval_ctx = session.eval_empty();
		let col = evaluate(&eval_ctx, expr)?;
		event_columns.push(Column::new(Fragment::internal(field_name), col.data));
	}
	let event_payload = Columns::new(event_columns);

	tx.record_test_event(
		plan.namespace.name().to_string(),
		sumtype.name.clone(),
		plan.variant_name.clone(),
		dispatch_depth,
		event_payload.clone(),
	);

	// Fire each catalog (RQL) procedure in declaration order
	for procedure in &procedures {
		let compiled = services.compiler.compile(tx, &procedure.body)?;

		match compiled {
			CompilationResult::Ready(compiled_list) => {
				let handler_start = services.runtime_context.clock.instant();
				let saved_ip = vm.ip;

				// Enter handler scope
				vm.symbols.enter_scope(ScopeType::Function);
				for col in event_payload.columns.iter() {
					let var_name = format!("event_{}", col.name.text());
					let scalar = Columns::new(vec![col.clone()]);
					vm.symbols.set(
						var_name,
						Variable::Columns {
							columns: scalar,
							is_scalar: true,
						},
						true,
					)?;
				}

				let mut handler_result = Vec::new();
				for compiled_unit in compiled_list.iter() {
					vm.ip = 0;
					if let Err(e) = vm.run(
						services,
						tx,
						&compiled_unit.instructions,
						params,
						&mut handler_result,
					) {
						tx.record_test_handler(CapturedInvocation {
							sequence: 0,
							namespace: plan.namespace.name().to_string(),
							handler: procedure.name.clone(),
							event: sumtype.name.clone(),
							variant: plan.variant_name.clone(),
							duration_ns: handler_start.elapsed().as_nanos() as u64,
							outcome: "error".to_string(),
							message: format!("{}", e),
						});
						return Err(e);
					}
				}

				vm.ip = saved_ip;
				let _ = vm.symbols.exit_scope();

				tx.record_test_handler(CapturedInvocation {
					sequence: 0,
					namespace: plan.namespace.name().to_string(),
					handler: procedure.name.clone(),
					event: sumtype.name.clone(),
					variant: plan.variant_name.clone(),
					duration_ns: handler_start.elapsed().as_nanos() as u64,
					outcome: "success".to_string(),
					message: String::new(),
				});
			}
			CompilationResult::Incremental(_) => {
				return Err(internal_error!("Handler body requires more input during dispatch"));
			}
		}
	}

	// Fire native (runtime-registered) handlers
	let native_handlers = services.get_handlers(variant_ref);
	let native_count = native_handlers.len();
	if !native_handlers.is_empty() {
		// Build named params from event payload (single-row columns → scalar values)
		let mut named_map = HashMap::new();
		for col in event_payload.columns.iter() {
			let key = col.name.text().to_string();
			if let Some(val) = col.data.iter().next() {
				named_map.insert(key, val);
			}
		}
		let call_params = Params::Named(Arc::new(named_map));

		for native_proc in native_handlers {
			let ctx = ProcedureContext {
				params: &call_params,
				catalog: &services.catalog,
				functions: &services.functions,
				runtime_context: &services.runtime_context,
				ioc: &services.ioc,
			};
			let handler_fragment =
				Fragment::internal(format!("handler for {}::{}", sumtype.name, plan.variant_name));
			let _result = native_proc.call(&ctx, tx).map_err(|e| e.with_context(handler_fragment))?;
		}
	}

	let total_fired = handler_count + native_count;
	Ok(Columns::single_row([("handlers_fired", Value::Uint1(total_fired as u8))]))
}
