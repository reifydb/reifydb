// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod ack;
pub mod claim;
pub mod extend;
pub mod replay;
pub mod token;

use reifydb_catalog::{
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::interface::catalog::{id::QueueId, queue::Queue};
use reifydb_routine_abi::error::RoutineError;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, value_type::ValueType},
};

pub(crate) fn require_command_transaction(procedure: &'static str, txn: &Transaction<'_>) -> Result<(), RoutineError> {
	if matches!(txn, Transaction::Query(..) | Transaction::Replica(..)) {
		return Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(procedure),
			reason: "must run in a command transaction".to_string(),
		});
	}
	Ok(())
}

pub(crate) fn utf8_arg(procedure: &'static str, value: &Value, argument_index: usize) -> Result<String, RoutineError> {
	match value {
		Value::Utf8(s) => Ok(s.clone()),
		other => Err(RoutineError::ProcedureInvalidArgumentType {
			procedure: Fragment::internal(procedure),
			argument_index,
			expected: vec![ValueType::Utf8],
			actual: other.get_type(),
		}),
	}
}

pub(crate) fn resolve_queue_by_name(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	qualified_name: &str,
	fragment: &Fragment,
) -> Result<Queue, RoutineError> {
	let Some((namespace_name, queue_name)) = Catalog::split_qualified_name(qualified_name) else {
		return Err(not_found("", qualified_name, fragment));
	};

	let Some(namespace) = catalog.find_namespace_by_name(txn, &namespace_name)? else {
		return Err(not_found(&namespace_name, queue_name, fragment));
	};

	catalog.find_queue_by_name(txn, namespace.id(), queue_name)?
		.ok_or_else(|| not_found(&namespace_name, queue_name, fragment))
}

pub(crate) fn resolve_queue_by_id(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	queue: QueueId,
	fragment: &Fragment,
) -> Result<Queue, RoutineError> {
	catalog.find_queue(txn, queue)?.ok_or_else(|| not_found("", &queue.0.to_string(), fragment))
}

fn not_found(namespace: &str, name: &str, fragment: &Fragment) -> RoutineError {
	CatalogError::NotFound {
		kind: CatalogObjectKind::Queue,
		namespace: namespace.to_string(),
		name: name.to_string(),
		fragment: fragment.clone(),
	}
	.into()
}
