// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::{Catalog, flow::FlowToCreate, view::ViewColumnToCreate},
	vtable::system::operator_libary::OperatorLibrary,
};
use reifydb_core::{
	common::{OperatorClass, TimeDomain},
	error::diagnostic::{
		flow::{flow_managed_operator_requires_event_time, flow_view_calls_script_routine},
		query,
	},
	interface::catalog::{
		column::ColumnIndex,
		flow::FlowStatus,
		view::{View, ViewSortKey},
	},
	sort::SortKey,
};
use reifydb_evaluate::{
	expression::{compile::compile_expression, context::CompileContext, udf_extract::extract_udf_calls},
	stack::SymbolTable,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::{
	expression::Expression,
	flow::{
		compiler::compile_flow,
		flow::FlowDag,
		operator::OperatorDef,
		time_domain::{check_join_retention_requirements, check_window_time_requirements, source_time_domain},
	},
	query::{QueryPlan, extract_resolved_source},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error, fragment::Fragment};

use crate::{
	Result,
	vm::volcano::{
		filter::resolve_variant_equality,
		inline::{resolve_is_variants, resolve_sumtype_ctor},
	},
};

fn outermost_sort(plan: &QueryPlan) -> Option<&Vec<SortKey>> {
	match plan {
		QueryPlan::Sort(node) => Some(&node.by),
		QueryPlan::Map(node) => node.input.as_deref().and_then(outermost_sort),
		QueryPlan::Extend(node) => node.input.as_deref().and_then(outermost_sort),
		QueryPlan::Filter(node) => outermost_sort(&node.input),
		QueryPlan::Take(node) => outermost_sort(&node.input),
		QueryPlan::Distinct(node) => outermost_sort(&node.input),
		_ => None,
	}
}

pub(crate) fn extract_view_sort(as_clause: &QueryPlan, columns: &[ViewColumnToCreate]) -> Vec<ViewSortKey> {
	let Some(by) = outermost_sort(as_clause) else {
		return Vec::new();
	};

	let mut resolved = Vec::with_capacity(by.len());
	for key in by {
		let Some(position) = columns.iter().position(|c| c.name.text() == key.column.text()) else {
			return Vec::new();
		};
		resolved.push(ViewSortKey {
			column: ColumnIndex(position as u8),
			direction: key.direction.clone(),
		});
	}
	resolved
}

pub mod authentication;
pub mod binding;
pub mod deferred;
pub mod dictionary;
pub mod event;

pub mod identity;
pub mod identity_attribute;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod property;
pub mod queue;
pub mod relationship;
pub mod remote_namespace;
pub mod ringbuffer;
pub mod role;
pub mod series;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod table;
pub mod tag;
pub mod test;
pub mod transactional;

fn plan_expressions_and_inputs(plan: &mut QueryPlan) -> (Vec<&mut Expression>, Vec<&mut QueryPlan>) {
	match plan {
		QueryPlan::Filter(node) => (node.conditions.iter_mut().collect(), vec![&mut node.input]),
		QueryPlan::Gate(node) => (node.conditions.iter_mut().collect(), vec![&mut node.input]),
		QueryPlan::Map(node) => {
			(node.map.iter_mut().collect(), node.input.as_deref_mut().into_iter().collect())
		}
		QueryPlan::Extend(node) => {
			(node.extend.iter_mut().collect(), node.input.as_deref_mut().into_iter().collect())
		}
		QueryPlan::Patch(node) => {
			(node.assignments.iter_mut().collect(), node.input.as_deref_mut().into_iter().collect())
		}
		QueryPlan::Apply(node) => {
			(node.params.iter_mut().collect(), node.input.as_deref_mut().into_iter().collect())
		}
		QueryPlan::Assert(node) => {
			(node.conditions.iter_mut().collect(), node.input.as_deref_mut().into_iter().collect())
		}
		QueryPlan::Aggregate(node) => {
			(node.by.iter_mut().chain(&mut node.map).collect(), vec![&mut node.input])
		}
		QueryPlan::Window(node) => (
			node.group_by.iter_mut().chain(&mut node.aggregations).collect(),
			node.input.as_deref_mut().into_iter().collect(),
		),
		QueryPlan::JoinInner(node) => (node.on.iter_mut().collect(), vec![&mut node.left, &mut node.right]),
		QueryPlan::JoinLeft(node) => (node.on.iter_mut().collect(), vec![&mut node.left, &mut node.right]),
		QueryPlan::JoinNatural(node) => (Vec::new(), vec![&mut node.left, &mut node.right]),
		QueryPlan::Append(node) => (Vec::new(), vec![&mut node.left, &mut node.right]),
		QueryPlan::Distinct(node) => (Vec::new(), vec![&mut node.input]),
		QueryPlan::Sort(node) => (Vec::new(), vec![&mut node.input]),
		QueryPlan::Take(node) => (Vec::new(), vec![&mut node.input]),
		QueryPlan::Scalarize(node) => (Vec::new(), vec![&mut node.input]),
		QueryPlan::InlineData(node) => {
			(node.rows.iter_mut().flatten().map(|field| field.expression.as_mut()).collect(), Vec::new())
		}
		QueryPlan::Generator(node) => (node.expressions.iter_mut().collect(), Vec::new()),
		QueryPlan::CallFunction(node) => (node.arguments.iter_mut().collect(), Vec::new()),
		QueryPlan::RemoteScan(_)
		| QueryPlan::TableScan(_)
		| QueryPlan::TableVirtualScan(_)
		| QueryPlan::ViewScan(_)
		| QueryPlan::RingBufferScan(_)
		| QueryPlan::DictionaryScan(_)
		| QueryPlan::SeriesScan(_)
		| QueryPlan::QueueScan(_)
		| QueryPlan::IndexScan(_)
		| QueryPlan::RowPointLookup(_)
		| QueryPlan::RowListLookup(_)
		| QueryPlan::RowRangeScan(_)
		| QueryPlan::Variable(_)
		| QueryPlan::Environment(_)
		| QueryPlan::RunTests(_) => (Vec::new(), Vec::new()),
	}
}

fn ensure_no_script_routine_call(plan: &mut QueryPlan, symbols: &SymbolTable) -> Result<()> {
	let (expressions, inputs) = plan_expressions_and_inputs(plan);
	for expression in expressions {
		let (_, calls) = extract_udf_calls(expression, symbols, &mut 0);
		if let Some(call) = calls.into_iter().next() {
			return Err(error!(flow_view_calls_script_routine(&call.name, call.fragment)));
		}
	}
	inputs.into_iter().try_for_each(|input| ensure_no_script_routine_call(input, symbols))
}

fn resolve_flow_variants(catalog: &Catalog, txn: &mut AdminTransaction, plan: &mut QueryPlan) -> Result<()> {
	let filter = matches!(plan, QueryPlan::Filter(_));
	let projection = matches!(plan, QueryPlan::Map(_) | QueryPlan::Extend(_));
	let (expressions, inputs) = plan_expressions_and_inputs(plan);
	if filter || projection {
		let source = inputs.first().and_then(|input| extract_resolved_source(input));
		for expression in expressions {
			if let Expression::Alias(alias) = &*expression
				&& let Expression::SumTypeConstructor(ctor) = alias.expression.as_ref()
			{
				resolve_sumtype_ctor(
					catalog,
					&mut Transaction::Admin(txn),
					None,
					&alias.alias.0,
					ctor,
				)?;
			}
			if let Some(source) = source.as_ref() {
				resolve_is_variants(catalog, &mut Transaction::Admin(txn), source, expression)?;
				if filter {
					resolve_variant_equality(
						catalog,
						&mut Transaction::Admin(txn),
						source,
						expression,
					)?;
				}
			}
		}
	}
	inputs.into_iter().try_for_each(|input| resolve_flow_variants(catalog, txn, input))
}

fn ensure_flow_expressions_compile(plan: &mut QueryPlan, symbols: &SymbolTable) -> Result<()> {
	let (expressions, inputs) = plan_expressions_and_inputs(plan);
	let compile_ctx = CompileContext {
		symbols,
	};
	for expression in expressions {
		compile_expression(&compile_ctx, expression)?;
	}
	inputs.into_iter().try_for_each(|input| ensure_flow_expressions_compile(input, symbols))
}

fn ensure_apply_operators_registered(plan: &mut QueryPlan, operators: &OperatorLibrary) -> Result<()> {
	if let QueryPlan::Apply(node) = plan
		&& operators.get(node.operator.text()).is_none()
	{
		return Err(error!(query::unknown_apply_operator(node.operator.clone())));
	}
	let (_, inputs) = plan_expressions_and_inputs(plan);
	inputs.into_iter().try_for_each(|input| ensure_apply_operators_registered(input, operators))
}

fn check_managed_time_requirements(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	flow: &FlowDag,
	operators: &OperatorLibrary,
) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);
	for operator_id in flow.topological_order() {
		let Some(node) = flow.get_operator(operator_id) else {
			continue;
		};
		let OperatorDef::Apply {
			operator,
			..
		} = &node.ty
		else {
			continue;
		};
		if operators.get(operator).and_then(|info| info.class) != Some(OperatorClass::Managed) {
			continue;
		}
		if source_time_domain(catalog, txn, flow)? != TimeDomain::Event {
			return Err(error!(flow_managed_operator_requires_event_time(&flow_name, operator)));
		}
		return Ok(());
	}
	Ok(())
}

pub(crate) fn create_deferred_view_flow(
	catalog: &Catalog,
	routines: &Routines,
	operators: &OperatorLibrary,
	txn: &mut AdminTransaction,
	symbols: &SymbolTable,
	view: &View,
	mut plan: QueryPlan,
) -> Result<()> {
	ensure_no_script_routine_call(&mut plan, symbols)?;
	ensure_apply_operators_registered(&mut plan, operators)?;
	resolve_flow_variants(catalog, txn, &mut plan)?;
	ensure_flow_expressions_compile(&mut plan, symbols)?;
	let flow = catalog.create_flow(
		txn,
		FlowToCreate {
			name: Fragment::internal(view.name()),
			namespace: view.namespace(),
			status: FlowStatus::Active,
		},
	)?;

	let dag = compile_flow(catalog, routines, txn, plan, Some(view), flow.id)?;
	check_window_time_requirements(catalog, &mut Transaction::Admin(txn), &dag)?;
	check_join_retention_requirements(catalog, &mut Transaction::Admin(txn), &dag)?;
	check_managed_time_requirements(catalog, &mut Transaction::Admin(txn), &dag, operators)
}
