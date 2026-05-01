// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		engine,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			ringbuffer::{PartitionedMetadata, RingBuffer},
		},
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedRingBuffer, ResolvedShape},
	},
	internal_error,
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::UpdateRingBufferNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use super::{
	coerce::coerce_value_to_column_type,
	context::RingBufferTarget,
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::{dictionary::DictionaryOperations, ringbuffer::RingBufferOperations},
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

pub(crate) fn update_ringbuffer(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateRingBufferNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let UpdateRingBufferNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, ringbuffer) = resolve_update_ringbuffer_target(services, txn, &target)?;
	let partitions = services.catalog.list_ringbuffer_partitions(txn, &ringbuffer)?;
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;
	let target_data = RingBufferTarget {
		namespace: &namespace,
		ringbuffer: &ringbuffer,
	};
	let context = build_update_ringbuffer_query_context(services, &target_data, &params, symbols);

	let mut input_node = compile(*input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let mut updated_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
	let has_returning = returning.is_some();

	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace.name(),
			&ringbuffer.name,
			DataOp::Update,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;
		if columns.row_numbers.is_empty() {
			return_error!(engine::missing_row_number_column());
		}
		let row_numbers = columns.row_numbers.clone();
		let row_count = columns.row_count();
		let mut column_map: HashMap<&str, usize> = HashMap::new();
		for (idx, col) in columns.iter().enumerate() {
			column_map.insert(col.name().text(), idx);
		}
		let view = ColumnView {
			columns: &columns,
			column_map: &column_map,
		};

		for row_idx in 0..row_count {
			let mut row = build_updated_ringbuffer_row(
				services,
				txn,
				&target_data,
				&shape,
				&view,
				&context,
				row_idx,
			)?;
			let row_number = row_numbers[row_idx];
			let old_row_key = RowKey::encoded(ringbuffer.id, row_number);
			let old_created_at =
				txn.get(&old_row_key)?.expect("row must exist for update").row.created_at_nanos();
			row.set_timestamps(old_created_at, services.runtime_context.clock.now_nanos());

			if !row_belongs_to_any_partition(&partitions, row_number) {
				continue;
			}

			let stored_row = txn.update_ringbuffer(ringbuffer.clone(), row_number, row)?;
			if has_returning {
				returned_rows.push((row_number, stored_row));
			}
			updated_count += 1;
		}
	}

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(update_ringbuffer_result(namespace.name(), &ringbuffer.name, updated_count))
}

/// Input columns + an index over them by name.
struct ColumnView<'a> {
	columns: &'a Columns,
	column_map: &'a HashMap<&'a str, usize>,
}

#[inline]
fn resolve_update_ringbuffer_target(
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
fn build_update_ringbuffer_query_context(
	services: &Arc<Services>,
	target: &RingBufferTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
) -> QueryContext {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let rb_ident = Fragment::internal(target.ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, target.ringbuffer.clone());
	QueryContext {
		services: services.clone(),
		source: Some(ResolvedShape::RingBuffer(resolved_rb)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	}
}

#[inline]
fn build_updated_ringbuffer_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	shape: &RowShape,
	view: &ColumnView<'_>,
	context: &QueryContext,
	row_idx: usize,
) -> Result<EncodedRow> {
	let mut row = shape.allocate();
	for (rb_idx, rb_column) in target.ringbuffer.columns.iter().enumerate() {
		let mut value = if let Some(&input_idx) = view.column_map.get(rb_column.name.as_str()) {
			view.columns[input_idx].get_value(row_idx)
		} else {
			Value::none()
		};

		let column_ident = view
			.columns
			.iter()
			.find(|col| col.name() == rb_column.name)
			.map(|col| col.name().clone())
			.unwrap_or_else(|| Fragment::internal(&rb_column.name));
		let resolved_column =
			ResolvedColumn::new(column_ident.clone(), context.source.clone().unwrap(), rb_column.clone());

		value = coerce_value_to_column_type(value, rb_column.constraint.get_type(), resolved_column, context)?;
		if let Err(mut e) = rb_column.constraint.validate(&value) {
			e.0.fragment = column_ident.clone();
			return Err(e);
		}

		let value = if let Some(dict_id) = rb_column.dictionary_id {
			let dictionary = services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
				internal_error!("Dictionary {:?} not found for column {}", dict_id, rb_column.name)
			})?;
			let entry_id = txn.insert_into_dictionary(&dictionary, &value)?;
			entry_id.to_value()
		} else {
			value
		};

		shape.set_value(&mut row, rb_idx, &value);
	}
	Ok(row)
}

#[inline]
fn row_belongs_to_any_partition(partitions: &[PartitionedMetadata], row_number: RowNumber) -> bool {
	partitions
		.iter()
		.any(|p| !p.metadata.is_empty() && row_number.0 >= p.metadata.head && row_number.0 < p.metadata.tail)
}

#[inline]
fn update_ringbuffer_result(namespace: &str, ringbuffer: &str, updated: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.to_string())),
		("updated", Value::Uint8(updated)),
	])
}
