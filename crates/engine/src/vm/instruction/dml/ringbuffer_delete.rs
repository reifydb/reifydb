// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use arrow_array::RecordBatch;
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		engine,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			ringbuffer::RingBuffer,
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedRingBuffer},
	},
	internal_err,
	ring::Ring,
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::{nodes::DeleteRingBufferNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value,
		identity::IdentityId,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{SystemColumn, partitions, row_numbers},
	},
};

use super::{
	context::{RingBufferTarget, WriteExecCtx},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_pre_image},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::ringbuffer::{RingBufferOperations, RingTxn, save_ring},
	vm::{
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

pub(crate) fn delete_ringbuffer(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteRingBufferNode,
	params: Params,
	symbols: &SymbolTable,
	caller: IdentityId,
) -> Result<RecordBatch> {
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
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;

	let exec = WriteExecCtx {
		services,
		symbols,
	};
	let rows_filter = if let Some(input_plan) = input {
		Some(collect_rows_for_ringbuffer_delete(
			&exec,
			txn,
			*input_plan,
			&target_data,
			&resolved_source,
			&params,
			caller,
		)?)
	} else {
		None
	};

	let (deleted_count, returned_rows) =
		delete_ringbuffer_rows(services, txn, &target_data, rows_filter, returning.is_some())?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &ringbuffer.columns, columns)?;
		let columns = with_pre_image(columns.clone(), &columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, caller);
	}
	delete_ringbuffer_result(namespace.name(), &ringbuffer.name, deleted_count)
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
fn build_delete_ringbuffer_resolved_source(namespace: &Namespace, ringbuffer: &RingBuffer) -> Option<ResolvedObject> {
	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let rb_ident = Fragment::internal(ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
	Some(ResolvedObject::RingBuffer(resolved_rb))
}

fn collect_rows_for_ringbuffer_delete(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_plan: QueryPlan,
	target: &RingBufferTarget<'_>,
	resolved_source: &Option<ResolvedObject>,
	params: &Params,
	caller: IdentityId,
) -> Result<Vec<(Option<Partition>, RowNumber)>> {
	let mut seen = HashSet::new();
	let mut rows_to_delete = Vec::new();
	let partitioned = !target.ringbuffer.partition_by.is_empty();

	let batch_size = exec.services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64;
	let mut input_node = compile(
		input_plan,
		txn,
		Arc::new(QueryContext {
			services: exec.services.clone(),
			source: resolved_source.clone(),
			batch_size,
			params: params.clone(),
			symbols: exec.symbols.clone(),
			identity: caller,
			memory: query_budget(exec.services),
		}),
	);

	let context = QueryContext {
		services: exec.services.clone(),
		source: None,
		batch_size,
		params: params.clone(),
		symbols: exec.symbols.clone(),
		identity: caller,
		memory: query_budget(exec.services),
	};
	input_node.initialize(txn, &context)?;

	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		if columns.num_rows() == 0 {
			continue;
		}
		PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.ringbuffer.name,
			DataOp::Delete,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;
		let numbers = row_numbers(&columns)?;
		if numbers.is_empty() {
			return_error!(engine::missing_row_number_column());
		}
		let sidecar = partitions(&columns)?;
		if partitioned && sidecar.len() != numbers.len() {
			return internal_err!(
				"a delete from partitioned ring buffer {} got rows without their {} column",
				target.ringbuffer.name,
				SystemColumn::Partitions
			);
		}
		for (idx, &row_number) in numbers.iter().enumerate() {
			let row = (sidecar.get(idx).copied(), row_number);
			if seen.insert(row) {
				rows_to_delete.push(row);
			}
		}
	}
	Ok(rows_to_delete)
}

fn delete_ringbuffer_rows(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	rows_filter: Option<Vec<(Option<Partition>, RowNumber)>>,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedBytes)>)> {
	let ringbuffer = target.ringbuffer;
	let catalog = &services.catalog;
	let mut ring = Ring::new(ringbuffer.capacity);
	let mut grouped: Vec<(Option<Partition>, Vec<RowNumber>)> = Vec::new();
	let mut position: HashMap<Option<Partition>, usize> = HashMap::new();
	match rows_filter {
		Some(rows) => {
			for (partition, row) in rows {
				let index = *position.entry(partition).or_insert_with(|| {
					grouped.push((partition, Vec::new()));
					grouped.len() - 1
				});
				grouped[index].1.push(row);
			}
		}
		None => {
			for listed in catalog.list_ringbuffer_partitions(txn, ringbuffer.id.into())? {
				let rows =
					listed.metadata.oldest_first(ringbuffer.capacity).map(|(_, row)| row).collect();
				grouped.push((listed.partition, rows));
			}
		}
	}

	let mut deleted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	for (partition, rows) in grouped {
		let mut store = RingTxn {
			catalog,
			txn: &mut *txn,
			ringbuffer,
		};
		let removed = ring.remove(&mut store, partition, &rows)?;
		let (doomed, doomed_rows): (Vec<RowNumber>, Vec<EncodedBytes>) = rows
			.into_iter()
			.zip(removed)
			.filter_map(|(row, bytes)| bytes.map(|bytes| (row, bytes)))
			.unzip();
		let doomed_partitions: Vec<Partition> =
			partition.map(|partition| vec![partition; doomed.len()]).unwrap_or_default();
		txn.remove_from_ringbuffer(ringbuffer, &doomed_partitions, &doomed, &doomed_rows)?;
		deleted_count += doomed.len() as u64;
		if has_returning {
			returned_rows.extend(doomed.into_iter().zip(doomed_rows));
		}
	}
	save_ring(catalog, txn, ringbuffer, &ring)?;
	Ok((deleted_count, returned_rows))
}

#[inline]
fn delete_ringbuffer_result(namespace: &str, ringbuffer: &str, deleted: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.to_string())),
		("deleted", Value::Uint8(deleted)),
	])
}
