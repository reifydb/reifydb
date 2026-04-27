// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashSet, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		engine,
	},
	interface::{
		catalog::{
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			ringbuffer::RingBuffer,
		},
		resolved::{ResolvedNamespace, ResolvedRingBuffer, ResolvedShape},
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_rql::{nodes::DeleteRingBufferNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use super::{
	context::{RingBufferTarget, WriteExecCtx},
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::ringbuffer::RingBufferOperations,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

pub(crate) fn delete_ringbuffer(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteRingBufferNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let DeleteRingBufferNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, ringbuffer) = resolve_delete_ringbuffer_target(services, txn, &target)?;
	let resolved_source = build_delete_ringbuffer_resolved_source(&namespace, &ringbuffer);
	let target_data = RingBufferTarget {
		namespace: &namespace,
		ringbuffer: &ringbuffer,
	};
	let partition_col_indices = compute_partition_col_indices(&ringbuffer);
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;

	let exec = WriteExecCtx {
		services,
		symbols,
	};
	let row_numbers_filter = if let Some(input_plan) = input {
		Some(collect_row_numbers_for_ringbuffer_delete(
			&exec,
			txn,
			*input_plan,
			&target_data,
			&resolved_source,
			&params,
		)?)
	} else {
		None
	};

	let (deleted_count, returned_rows) = delete_ringbuffer_partitions(
		services,
		txn,
		&target_data,
		&shape,
		&partition_col_indices,
		row_numbers_filter.as_ref(),
		returning.is_some(),
	)?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(delete_ringbuffer_result(namespace.name(), &ringbuffer.name, deleted_count))
}

#[inline]
fn resolve_delete_ringbuffer_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedRingBuffer,
) -> Result<(Namespace, RingBuffer)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let ringbuffer_name = target.name();
	let Some(ringbuffer) = services.catalog.find_ringbuffer_by_name(txn, namespace.id(), ringbuffer_name)? else {
		let fragment = Fragment::internal(target.name());
		return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
	};
	Ok((namespace, ringbuffer))
}

#[inline]
fn build_delete_ringbuffer_resolved_source(namespace: &Namespace, ringbuffer: &RingBuffer) -> Option<ResolvedShape> {
	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let rb_ident = Fragment::internal(ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
	Some(ResolvedShape::RingBuffer(resolved_rb))
}

#[inline]
fn compute_partition_col_indices(ringbuffer: &RingBuffer) -> Vec<usize> {
	ringbuffer
		.partition_by
		.iter()
		.map(|pb_col| ringbuffer.columns.iter().position(|c| c.name == *pb_col).unwrap())
		.collect()
}

fn collect_row_numbers_for_ringbuffer_delete(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_plan: QueryPlan,
	target: &RingBufferTarget<'_>,
	resolved_source: &Option<ResolvedShape>,
	params: &Params,
) -> Result<HashSet<RowNumber>> {
	let mut row_numbers_to_delete = HashSet::new();

	let mut input_node = compile(
		input_plan,
		txn,
		Arc::new(QueryContext {
			services: exec.services.clone(),
			source: resolved_source.clone(),
			batch_size: 1024,
			params: params.clone(),
			symbols: exec.symbols.clone(),
			identity: IdentityId::root(),
		}),
	);

	let context = QueryContext {
		services: exec.services.clone(),
		source: None,
		batch_size: 1024,
		params: params.clone(),
		symbols: exec.symbols.clone(),
		identity: IdentityId::root(),
	};
	input_node.initialize(txn, &context)?;

	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.ringbuffer.name,
			DataOp::Delete,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;
		if columns.row_numbers.is_empty() {
			return_error!(engine::missing_row_number_column());
		}
		row_numbers_to_delete.extend(columns.row_numbers.iter().copied());
	}
	Ok(row_numbers_to_delete)
}

fn delete_ringbuffer_partitions(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	shape: &RowShape,
	partition_col_indices: &[usize],
	row_numbers_filter: Option<&HashSet<RowNumber>>,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedRow)>)> {
	let ringbuffer = target.ringbuffer;
	let mut deleted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
	let partitions = services.catalog.list_ringbuffer_partitions(txn, ringbuffer)?;

	for partition_info in partitions {
		let partition_key = partition_info.partition_values.clone();
		let mut partition = partition_info.metadata;
		let mut min_remaining_row: Option<u64> = None;
		let mut partition_deleted = 0u64;

		for row_num_value in partition.head..partition.tail {
			let row_num = RowNumber(row_num_value);
			let key = RowKey::encoded(ringbuffer.id, row_num);
			let Some(row_data) = txn.get(&key)? else {
				continue;
			};
			if !partition_col_indices.is_empty()
				&& !row_matches_partition(shape, &row_data.row, partition_col_indices, &partition_key)
			{
				continue;
			}
			let should_delete = match row_numbers_filter {
				Some(filter) => filter.contains(&row_num),
				None => true,
			};
			if should_delete {
				let deleted_values = txn.remove_from_ringbuffer(ringbuffer, row_num)?;
				if has_returning {
					returned_rows.push((row_num, deleted_values));
				}
				partition_deleted += 1;
				deleted_count += 1;
			} else {
				min_remaining_row =
					Some(min_remaining_row.map_or(row_num_value, |m: u64| m.min(row_num_value)));
			}
		}

		if row_numbers_filter.is_some() {
			if partition_deleted > 0 {
				let remaining_count = partition.count.saturating_sub(partition_deleted);
				if remaining_count == 0 {
					partition.count = 0;
					partition.head = partition.tail;
				} else {
					partition.count = remaining_count;
					partition.head = min_remaining_row.unwrap();
				}
				services.catalog.save_partition_metadata(
					txn,
					ringbuffer,
					&partition_key,
					&partition,
				)?;
			}
		} else {
			partition.count = 0;
			partition.head = partition.tail;
			services.catalog.save_partition_metadata(txn, ringbuffer, &partition_key, &partition)?;
		}
	}
	Ok((deleted_count, returned_rows))
}

#[inline]
fn delete_ringbuffer_result(namespace: &str, ringbuffer: &str, deleted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.to_string())),
		("deleted", Value::Uint8(deleted)),
	])
}

fn row_matches_partition(
	shape: &RowShape,
	row: &EncodedRow,
	partition_col_indices: &[usize],
	expected_values: &[Value],
) -> bool {
	partition_col_indices.iter().zip(expected_values).all(|(&idx, expected)| shape.get_value(row, idx) == *expected)
}
