// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use postcard::to_stdvec;
use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
use reifydb_core::{
	error::diagnostic::catalog::{
		namespace_not_found, queue_deduplication_key_not_utf8, queue_not_before_not_datetime, queue_not_found,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			queue::Queue,
		},
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedObject, ResolvedQueue},
	},
	internal_error,
	key::{queue_deduplication::QueueDeduplicationKey, row::RowKey},
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{
	expression::Expression,
	nodes::{InsertQueueNode, QUEUE_CREATED_COLUMN, QUEUE_DEDUPLICATION_KEY_FIELD, QUEUE_NOT_BEFORE_FIELD},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value, datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition,
		row_number::RowNumber, value_type::ValueType,
	},
};
use tracing::instrument;

use super::{
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_queue_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::{
		dictionary::DictionaryOperations,
		queue::{QueueInsertRow, QueueOperations},
	},
	vm::{
		instruction::dml::{coerce::coerce_value_to_column_type, time::resolve_time},
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

struct QueueTarget<'a> {
	namespace: &'a Namespace,
	queue: &'a Queue,
}

#[instrument(name = "mutate::queue::insert", level = "trace", skip_all)]
pub(crate) fn insert_queue(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertQueueNode,
	symbols: &mut SymbolTable,
) -> Result<Columns> {
	let InsertQueueNode {
		input,
		target,
		has_deduplication,
		has_not_before,
		returning,
	} = plan;

	let (namespace, queue) = resolve_insert_queue_target(services, txn, &target)?;
	let shape = get_or_create_queue_shape(&services.catalog, &queue, txn)?;
	let target_data = QueueTarget {
		namespace: &namespace,
		queue: &queue,
	};

	let context = build_insert_queue_query_context(services, &target_data, symbols);
	let mut input_node = compile(*input, txn, context.clone());
	input_node.initialize(txn, &context)?;

	let pending = validate_and_encode_input_rows(
		services,
		txn,
		&target_data,
		&shape,
		&context,
		symbols,
		&mut input_node,
		has_deduplication,
		has_not_before,
	)?;

	if pending.is_empty() {
		return Ok(insert_queue_result(namespace.name(), &queue.name, 0, 0));
	}

	let now = services.runtime_context.clock.now();
	let outcomes = resolve_duplicates(txn, &queue, &shape, &pending, now)?;

	let fresh_count = outcomes.iter().filter(|outcome| matches!(outcome, Outcome::Fresh)).count();
	let duplicates = outcomes.len() - fresh_count;

	let row_numbers = if fresh_count == 0 {
		Vec::new()
	} else {
		services.catalog.next_row_number_batch_for_queue(txn, queue.id, fresh_count as u64)?
	};

	let ordered_by_index = ordered_by_index(&queue)?;
	let mut assigned = row_numbers.into_iter();
	let mut rows: Vec<QueueInsertRow> = Vec::with_capacity(fresh_count);
	let mut returned: Vec<ReturnedRow> = Vec::with_capacity(outcomes.len());

	for (item, outcome) in pending.iter().zip(outcomes.into_iter()) {
		match outcome {
			Outcome::Fresh => {
				let row_number = assigned.next().expect("a row number per fresh item");
				if let Some(key) = &item.deduplication_key {
					write_deduplication_record(txn, &queue, key, row_number, now)?;
				}
				rows.push(QueueInsertRow {
					row_number,
					partition: partition_of(
						&queue,
						&shape,
						&item.encoded,
						ordered_by_index,
						row_number,
					),
					not_before: item.not_before,
					encoded: item.encoded.clone(),
				});
				returned.push(ReturnedRow {
					created: true,
					row_number,
					encoded: item.encoded.clone(),
				});
			}
			Outcome::Duplicate {
				row_number,
				encoded,
			} => returned.push(ReturnedRow {
				created: false,
				row_number,
				encoded: encoded.unwrap_or_else(|| shape.allocate().freeze()),
			}),
		}
	}

	txn.insert_queue(&queue, &rows)?;

	if let Some(returning_exprs) = &returning {
		return project_returning(services, txn, symbols, &queue, &shape, returning_exprs, &returned);
	}

	Ok(insert_queue_result(namespace.name(), &queue.name, fresh_count as u64, duplicates as u64))
}

struct PendingItem {
	encoded: EncodedRow,
	deduplication_key: Option<Vec<u8>>,
	not_before: Option<DateTime>,
}

enum Outcome {
	Fresh,
	Duplicate {
		row_number: RowNumber,
		encoded: Option<EncodedRow>,
	},
}

struct ReturnedRow {
	created: bool,
	row_number: RowNumber,
	encoded: EncodedRow,
}

fn deduplication_record_shape() -> RowShape {
	RowShape::testing(&[ValueType::Uint8, ValueType::DateTime])
}

fn write_deduplication_record(
	txn: &mut Transaction<'_>,
	queue: &Queue,
	key: &[u8],
	row_number: RowNumber,
	now: DateTime,
) -> Result<()> {
	let ttl = queue.deduplicate.as_ref().map(|d| d.ttl).unwrap_or(Duration::MAX);
	let shape = deduplication_record_shape();
	let mut record = shape.allocate();
	shape.set::<u64>(&mut record, 0, row_number.0);
	shape.set_value(&mut record, 1, &Value::DateTime(now.saturating_add(ttl)));
	txn.set(&QueueDeduplicationKey::encoded(queue.id, key.to_vec()), record.freeze())?;
	Ok(())
}

fn resolve_duplicates(
	txn: &mut Transaction<'_>,
	queue: &Queue,
	shape: &RowShape,
	pending: &[PendingItem],
	now: DateTime,
) -> Result<Vec<Outcome>> {
	let record_shape = deduplication_record_shape();
	let mut outcomes = Vec::with_capacity(pending.len());
	let mut seen: HashMap<Vec<u8>, RowNumber> = HashMap::new();

	for item in pending {
		let Some(key) = &item.deduplication_key else {
			outcomes.push(Outcome::Fresh);
			continue;
		};

		if let Some(&row_number) = seen.get(key) {
			outcomes.push(Outcome::Duplicate {
				row_number,
				encoded: None,
			});
			continue;
		}

		let stored = txn.get(&QueueDeduplicationKey::encoded(queue.id, key.clone()))?;
		if let Some(stored) = stored {
			let row_number = RowNumber(record_shape.get::<u64>(&stored.row, 0));
			let expires_at = record_shape.get_value(&stored.row, 1);
			if !has_expired(&expires_at, now) {
				let encoded = txn.get(&RowKey::encoded(queue.id, row_number))?.map(|item| item.row);
				outcomes.push(Outcome::Duplicate {
					row_number,
					encoded,
				});
				continue;
			}
		}

		seen.insert(key.clone(), RowNumber(0));
		outcomes.push(Outcome::Fresh);
	}

	let _ = shape;
	Ok(outcomes)
}

#[inline]
fn has_expired(expires_at: &Value, now: DateTime) -> bool {
	match expires_at {
		Value::DateTime(instant) => *instant <= now,
		_ => false,
	}
}

fn project_returning(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	symbols: &mut SymbolTable,
	queue: &Queue,
	shape: &RowShape,
	returning_exprs: &[Expression],
	returned: &[ReturnedRow],
) -> Result<Columns> {
	let rows: Vec<(RowNumber, EncodedRow)> =
		returned.iter().map(|row| (row.row_number, row.encoded.clone())).collect();
	let mut columns = decode_rows_to_columns(shape, &rows);
	truncate_to_declared(&mut columns, queue.columns.len());
	decode_returning_dictionaries(services, txn, &queue.columns, &mut columns)?;

	let mut created = ColumnBuffer::bool_with_capacity(returned.len());
	for row in returned {
		created.push_value(Value::Boolean(row.created));
	}
	columns.columns.push(created);
	columns.names.push(Fragment::internal(QUEUE_CREATED_COLUMN));

	evaluate_returning(services, symbols, returning_exprs, columns)
}

fn declared_key_indices(queue: &Queue) -> Result<Option<Vec<usize>>> {
	let Some(deduplicate) = &queue.deduplicate else {
		return Ok(None);
	};
	let mut indices = Vec::with_capacity(deduplicate.by.len());
	for column in &deduplicate.by {
		let index = queue.columns.iter().position(|c| c.name == *column).ok_or_else(|| {
			internal_error!("queue {} deduplicates by {} which is not a column", queue.name, column)
		})?;
		indices.push(index);
	}
	Ok(Some(indices))
}

fn declared_key_bytes(shape: &RowShape, row: &EncodedRow, indices: &[usize]) -> Vec<u8> {
	let values: Vec<Value> = indices.iter().map(|&index| shape.get_value(row, index)).collect();
	to_stdvec(&values).expect("postcard serialization of a Value list is total")
}

#[inline]
fn ordered_by_index(queue: &Queue) -> Result<Option<usize>> {
	let Some(ordered_by) = queue.ordered_by() else {
		return Ok(None);
	};
	let index = queue.columns.iter().position(|c| c.name == *ordered_by).ok_or_else(|| {
		internal_error!("queue {} declares ordered_by {} which is not a column", queue.name, ordered_by)
	})?;
	Ok(Some(index))
}

#[inline]
fn partition_of(
	queue: &Queue,
	shape: &RowShape,
	row: &EncodedRow,
	ordered_by_index: Option<usize>,
	row_number: RowNumber,
) -> u16 {
	let hash = match ordered_by_index {
		Some(index) => Partition::of(&[shape.get_value(row, index)]),
		None => Partition::of(&[Value::Uint8(row_number.0)]),
	};
	(hash.0 % queue.partitions() as u128) as u16
}

#[inline]
fn truncate_to_declared(columns: &mut Columns, declared: usize) {
	columns.columns.truncate(declared);
	columns.names.truncate(declared);
}

#[inline]
fn resolve_insert_queue_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedQueue,
) -> Result<(Namespace, Queue)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let queue_name = target.name();
	let Some(queue) = services.catalog.find_queue_by_name(txn, namespace.id(), queue_name)? else {
		return_error!(queue_not_found(target.identifier().clone(), namespace_name, queue_name));
	};
	Ok((namespace, queue))
}

#[inline]
fn build_insert_queue_query_context(
	services: &Arc<Services>,
	target: &QueueTarget<'_>,
	symbols: &SymbolTable,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let queue_ident = Fragment::internal(target.queue.name.clone());
	let resolved_queue = ResolvedQueue::new(queue_ident, resolved_namespace, target.queue.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::Queue(resolved_queue)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: Params::None,
		symbols: symbols.clone(),
		identity: IdentityId::root(),
		memory: query_budget(services),
	})
}

#[allow(clippy::too_many_arguments)]
fn validate_and_encode_input_rows(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &QueueTarget<'_>,
	shape: &RowShape,
	context: &Arc<QueryContext>,
	symbols: &SymbolTable,
	input_node: &mut Box<dyn QueryNode>,
	has_deduplication: bool,
	has_not_before: bool,
) -> Result<Vec<PendingItem>> {
	let mut pending: Vec<PendingItem> = Vec::new();
	let mut mutable_context = (**context).clone();
	let declared_key_indices = declared_key_indices(target.queue)?;

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.queue.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Queue,
		)?;

		let mut column_map: HashMap<&str, usize> = HashMap::new();
		for (idx, col) in columns.iter().enumerate() {
			column_map.insert(col.name().text(), idx);
		}

		for row_idx in 0..columns.row_count() {
			let declared_key_indices = declared_key_indices.as_deref();
			let not_before = if has_not_before {
				read_not_before(target, &columns, &column_map, row_idx)?
			} else {
				None
			};

			let encoded = build_insert_queue_row(
				services,
				txn,
				target,
				shape,
				&columns,
				&column_map,
				context,
				row_idx,
				not_before,
			)?;

			let deduplication_key = match declared_key_indices {
				Some(indices) => Some(declared_key_bytes(shape, &encoded, indices)),
				None if has_deduplication => {
					read_deduplication_key(target, &columns, &column_map, row_idx)?
				}
				None => None,
			};

			pending.push(PendingItem {
				encoded,
				deduplication_key,
				not_before,
			});
		}
	}

	Ok(pending)
}

#[inline]
fn read_deduplication_key(
	target: &QueueTarget<'_>,
	columns: &Columns,
	column_map: &HashMap<&str, usize>,
	row_idx: usize,
) -> Result<Option<Vec<u8>>> {
	let Some(&idx) = column_map.get(QUEUE_DEDUPLICATION_KEY_FIELD) else {
		return Ok(None);
	};
	match columns[idx].get_value(row_idx) {
		Value::None {
			..
		} => Ok(None),
		Value::Utf8(text) => Ok(Some(text.into_bytes())),
		other => return_error!(queue_deduplication_key_not_utf8(
			Fragment::internal(target.queue.name.clone()),
			other.get_type().to_string().as_str()
		)),
	}
}

#[inline]
fn read_not_before(
	target: &QueueTarget<'_>,
	columns: &Columns,
	column_map: &HashMap<&str, usize>,
	row_idx: usize,
) -> Result<Option<DateTime>> {
	let Some(&idx) = column_map.get(QUEUE_NOT_BEFORE_FIELD) else {
		return Ok(None);
	};
	match columns[idx].get_value(row_idx) {
		Value::None {
			..
		} => Ok(None),
		Value::DateTime(instant) => Ok(Some(instant)),
		other => return_error!(queue_not_before_not_datetime(
			Fragment::internal(target.queue.name.clone()),
			other.get_type().to_string().as_str()
		)),
	}
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn build_insert_queue_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &QueueTarget<'_>,
	shape: &RowShape,
	columns: &Columns,
	column_map: &HashMap<&str, usize>,
	context: &Arc<QueryContext>,
	row_idx: usize,
	not_before: Option<DateTime>,
) -> Result<EncodedRow> {
	let mut row = shape.allocate();

	for (queue_idx, queue_column) in target.queue.columns.iter().enumerate() {
		let mut value = if let Some(&input_idx) = column_map.get(queue_column.name.as_str()) {
			columns[input_idx].get_value(row_idx)
		} else {
			Value::none()
		};

		if queue_column.auto_increment && matches!(value, Value::None { .. }) {
			value = services.catalog.column_sequence_next_value(txn, target.queue.id, queue_column.id)?;
		}

		let column_ident = column_map
			.get(queue_column.name.as_str())
			.map(|&idx| columns.name_at(idx).clone())
			.unwrap_or_else(|| Fragment::internal(queue_column.name.clone()));

		let resolved_column = ResolvedColumn::new(
			column_ident.clone(),
			context.source.clone().unwrap(),
			queue_column.clone(),
		);

		value = coerce_value_to_column_type(
			value,
			queue_column.constraint.get_type(),
			resolved_column,
			context,
		)?;

		if let Err(mut e) = queue_column.constraint.validate(&value) {
			e.0.fragment = column_ident.clone();
			return Err(e);
		}

		let value = if let Some(dict_id) = queue_column.dictionary_id {
			let dictionary = services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
				internal_error!("Dictionary {:?} not found for column {}", dict_id, queue_column.name)
			})?;
			let entry_id = if matches!(value, Value::None { .. }) {
				dictionary.id_type.none()
			} else {
				txn.insert_into_dictionary(&dictionary, &value)?
			};
			entry_id.to_value()
		} else {
			value
		};

		shape.set_value(&mut row, queue_idx, &value);
	}

	let not_before_value = match not_before {
		Some(instant) => Value::DateTime(instant),
		None => Value::none(),
	};
	shape.set_value(&mut row, target.queue.columns.len(), &not_before_value);

	let now = services.runtime_context.clock.now();
	row.set_timestamps(now, now);
	row.set_time(resolve_time(&target.queue.name, &target.queue.columns, &target.queue.time, shape, &row, now)?);

	Ok(row.freeze())
}

#[inline]
fn insert_queue_result(namespace: &str, queue: &str, inserted: u64, duplicates: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("queue", Value::Utf8(queue.to_string())),
		("inserted", Value::Uint8(inserted)),
		("duplicates", Value::Uint8(duplicates)),
	])
}
