// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	internal_error,
	testing::CapturedInvocation,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_evaluate::{
	expression::{context::EvalContext, eval::evaluate},
	stack::Variable,
};
use reifydb_policy::inject_from_policies;
use reifydb_rql::{compiler::CompilationResult, instruction::ScopeType, nodes::DispatchNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, duration::Duration, sumtype::VariantRef},
};

use crate::{
	Result,
	vm::{
		callable::{CallSite, ProcedureCall, enforce_call_policy, invoke_procedure_routine},
		services::Services,
		vm::Vm,
	},
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

	let procedures = {
		let mut tx_tmp = tx.reborrow();
		services.catalog.list_procedures_for_variant(&mut tx_tmp, variant_ref)?
	};

	let handler_count = procedures.len();

	let base = EvalContext {
		params,
		symbols: &vm.symbols,
		routines: &services.routines,
		runtime_context: &services.runtime_context,
		identity: tx.identity(),
		is_aggregate_context: false,
		columns: Columns::empty(),
		row_count: 1,
		target: None,
		take: None,
	};
	let mut event_columns = Vec::with_capacity(plan.fields.len());
	for (field_name, expr) in &plan.fields {
		let eval_ctx = base.with_eval_empty();
		let col = evaluate(&eval_ctx, expr)?;
		event_columns.push(ColumnWithName::new(Fragment::internal(field_name), col.data));
	}
	let event_payload = Columns::new(event_columns);

	tx.record_test_event(
		plan.namespace.name().to_string(),
		sumtype.name.clone(),
		plan.variant_name.clone(),
		dispatch_depth,
		event_payload.clone(),
	);

	for procedure in &procedures {
		let handler_namespace = {
			let mut tx_tmp = tx.reborrow();
			services.catalog.get_namespace(&mut tx_tmp, procedure.namespace())?.name().to_string()
		};
		let handler_name = format!("{}::{}", handler_namespace, procedure.name());
		enforce_call_policy(
			services,
			&vm.symbols,
			tx,
			&handler_name,
			CallSite::EventHandler {
				event: &sumtype.name,
				variant: &plan.variant_name,
			},
		)?;

		let compiled = services.compiler.compile_with_policy(
			tx,
			procedure.body().unwrap_or_default(),
			inject_from_policies,
		)?;

		match compiled {
			CompilationResult::Ready(compiled_list) => {
				let handler_start = services.runtime_context.clock.instant();
				let saved_ip = vm.ip;

				vm.symbols.enter_scope(ScopeType::Function);
				for (idx, name) in event_payload.names.iter().enumerate() {
					let var_name = format!("event_{}", name.text());
					let scalar = Columns::new(vec![ColumnWithName::new(
						name.clone(),
						event_payload.columns[idx].clone(),
					)]);
					vm.symbols.set(var_name, Variable::columns(scalar), true)?;
				}

				let mut handler_result = Vec::new();
				for compiled_unit in compiled_list.iter() {
					vm.ip = 0;
					if let Err(e) =
						vm.run(services, tx, &compiled_unit.instructions, &mut handler_result)
					{
						tx.record_test_handler(CapturedInvocation {
							sequence: 0,
							namespace: plan.namespace.name().to_string(),
							handler: procedure.name().to_string(),
							event: sumtype.name.clone(),
							variant: plan.variant_name.clone(),
							duration: Duration::from_std(handler_start.elapsed()),
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
					handler: procedure.name().to_string(),
					event: sumtype.name.clone(),
					variant: plan.variant_name.clone(),
					duration: Duration::from_std(handler_start.elapsed()),
					outcome: "success".to_string(),
					message: String::new(),
				});
			}
			CompilationResult::Incremental(_) => {
				return Err(internal_error!("Handler body requires more input during dispatch"));
			}
		}
	}

	let native_handlers = services.get_handlers(tx, variant_ref);
	let native_count = native_handlers.len();
	if !native_handlers.is_empty() {
		let mut named_map = HashMap::new();
		for (idx, name) in event_payload.names.iter().enumerate() {
			let key = name.text().to_string();
			if let Some(val) = event_payload.columns[idx].iter().next() {
				named_map.insert(key, val);
			}
		}
		let call_params = Params::Named(Arc::new(named_map));

		for native_proc in native_handlers {
			let handler_fragment =
				Fragment::internal(format!("handler for {}::{}", sumtype.name, plan.variant_name));
			let handler_name = native_proc.info().name.clone();
			let _result = invoke_procedure_routine(
				services,
				&vm.symbols,
				tx,
				ProcedureCall {
					routine: &native_proc,
					fragment: &handler_fragment,
					target: &handler_name,
					params: &call_params,
				},
				CallSite::EventHandler {
					event: &sumtype.name,
					variant: &plan.variant_name,
				},
			)?;
		}
	}

	let total_fired = handler_count + native_count;
	Ok(Columns::single_row([("handlers_fired", Value::Uint1(total_fired as u8))]))
}
