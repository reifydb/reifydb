// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use postcard::to_stdvec;
use reifydb_codec::{
	key::serializer::KeySerializer,
	row::{
		bytes::{EncodedBytes, RowBuilder},
		queue::{EncodedQueueRow, EncodedQueueRowBuilder},
		queue_deduplication::EncodedQueueDeduplicationRow,
		shape::RowShape,
	},
};
use reifydb_core::{
	error::diagnostic::{
		catalog::{
			namespace_not_found, queue_deduplication_key_not_utf8, queue_not_before_not_datetime,
			queue_not_found,
		},
		query::column_not_found,
	},
	expression::Expression,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			queue::{Queue, decode_queue_deduplication, encode_queue_deduplication},
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedQueue},
	},
	internal_error,
	key::{queue::QueueDeduplicationKey, row::RowKey},
	return_internal_error,
	value::{
		batch::{batch, decode_cells, single_row},
		column::{builder::ColumnBuilder, key::extend_keys},
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::{
	InsertQueueNode, QUEUE_CREATED_COLUMN, QUEUE_DEDUPLICATION_KEY_FIELD, QUEUE_NOT_BEFORE_FIELD,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		datetime::DateTime,
		duration::Duration,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{SystemColumn, system_column, user_columns, with_system_column},
		value_type::ValueType,
	},
};
use tracing::instrument;

use super::{
	columns::{CastColumns, ColumnPipeline, Failure, input_views, intern_dictionary_columns},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_absent_pre_image},
	shape::get_or_create_queue_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	queue::partition::{ordered_by_index, placement_of},
	transaction::operation::queue::{QueueInsertRow, QueueOperations},
	vm::{
		instruction::dml::{
			coerce::InputFragments,
			time::{EventColumn, populator_index, resolve_time},
		},
		services::Services,
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
) -> Result<RecordBatch> {
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

	let context = build_insert_queue_query_context(services, &target_data, symbols, txn.identity());
	let fragments = InputFragments::of(&input);
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
		&fragments,
		has_deduplication,
		has_not_before,
	)?;

	if pending.is_empty() {
		return insert_queue_result(namespace.name(), &queue.name, 0, 0);
	}

	let now = services.runtime_context.clock.now();
	let outcomes = resolve_duplicates(txn, &queue, &pending, now)?;

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
				let placement = placement_of(
					&queue,
					&shape,
					EncodedQueueRow::view(&item.encoded),
					ordered_by_index,
					row_number,
				);
				rows.push(QueueInsertRow {
					row_number,
					partition: placement.partition,
					key_hash: placement.key_hash,
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
				encoded: encoded.unwrap_or_else(|| shape.allocate_queue().freeze_bytes()),
			}),
			Outcome::DuplicateInBatch {
				origin,
			} => {
				let row_number = returned[origin].row_number;
				let encoded = returned[origin].encoded.clone();
				returned.push(ReturnedRow {
					created: false,
					row_number,
					encoded,
				});
			}
		}
	}

	txn.insert_queue(&queue, &rows)?;

	if let Some(returning_exprs) = &returning {
		return project_returning(services, txn, symbols, &queue, &shape, returning_exprs, &returned);
	}

	insert_queue_result(namespace.name(), &queue.name, fresh_count as u64, duplicates as u64)
}

struct PendingItem {
	encoded: EncodedBytes,
	deduplication_key: Option<Vec<u8>>,
	not_before: Option<DateTime>,
}

enum Outcome {
	Fresh,
	Duplicate {
		row_number: RowNumber,
		encoded: Option<EncodedBytes>,
	},
	DuplicateInBatch {
		origin: usize,
	},
}

struct ReturnedRow {
	created: bool,
	row_number: RowNumber,
	encoded: EncodedBytes,
}

fn write_deduplication_record(
	txn: &mut Transaction<'_>,
	queue: &Queue,
	key: &[u8],
	row_number: RowNumber,
	now: DateTime,
) -> Result<()> {
	let ttl = queue.deduplicate.as_ref().map(|d| d.ttl).unwrap_or(Duration::MAX);
	let record = encode_queue_deduplication(row_number, now.saturating_add(ttl));
	txn.set(&QueueDeduplicationKey::new(queue.id, key), record.into_bytes())?;
	Ok(())
}

fn resolve_duplicates(
	txn: &mut Transaction<'_>,
	queue: &Queue,
	pending: &[PendingItem],
	now: DateTime,
) -> Result<Vec<Outcome>> {
	let mut outcomes = Vec::with_capacity(pending.len());
	let mut seen: HashMap<&[u8], usize> = HashMap::new();

	for (index, item) in pending.iter().enumerate() {
		let Some(key) = item.deduplication_key.as_deref() else {
			outcomes.push(Outcome::Fresh);
			continue;
		};

		if let Some(&origin) = seen.get(key) {
			outcomes.push(Outcome::DuplicateInBatch {
				origin,
			});
			continue;
		}

		let stored = txn.get(&QueueDeduplicationKey::new(queue.id, key))?;
		if let Some(stored) = stored {
			let Some((row_number, expires_at)) =
				decode_queue_deduplication(EncodedQueueDeduplicationRow::view(&stored.bytes))
			else {
				return_internal_error!(
					"Queue {} deduplication record is {} bytes wide, too short for its header. This indicates a corrupt record.",
					queue.name,
					stored.bytes.len()
				)
			};
			if expires_at > now {
				let encoded = txn.get(&RowKey::new(queue.id, row_number))?.map(|item| item.bytes);
				outcomes.push(Outcome::Duplicate {
					row_number,
					encoded,
				});
				continue;
			}
		}

		seen.insert(key, index);
		outcomes.push(Outcome::Fresh);
	}

	Ok(outcomes)
}

fn project_returning(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	symbols: &mut SymbolTable,
	queue: &Queue,
	shape: &RowShape,
	returning_exprs: &[Expression],
	returned: &[ReturnedRow],
) -> Result<RecordBatch> {
	let rows: Vec<(RowNumber, EncodedBytes)> =
		returned.iter().map(|row| (row.row_number, row.encoded.clone())).collect();
	let columns = decode_rows_to_columns(shape, &rows)?;
	let columns = truncate_to_declared(columns, queue.columns.len())?;
	let columns = decode_returning_dictionaries(services, txn, &queue.columns, columns)?;
	let columns = with_absent_pre_image(columns)?;

	let mut created = ColumnBuilder::with_capacity(ValueType::Boolean, returned.len());
	for row in returned {
		created.push_value(Value::Boolean(row.created));
	}
	let mut user: Vec<(FieldRef, ArrayRef)> =
		user_columns(&columns).map(|(field, array)| (field.clone(), array.clone())).collect();
	user.push(created.finish(QUEUE_CREATED_COLUMN));
	let columns = rebuild_with_user_columns(&columns, user)?;

	evaluate_returning(services, symbols, returning_exprs, columns, txn.identity())
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

fn declared_keys(shape: &RowShape, rows: &[EncodedBytes], indices: &[usize]) -> Result<Vec<Vec<u8>>> {
	let mut keys: Vec<KeySerializer> = rows.iter().map(|_| KeySerializer::new()).collect();
	for &index in indices {
		let field = &shape.fields()[index];
		let field_type = field.constraint.get_type();
		if matches!(
			field_type.inner_type(),
			ValueType::Digest { .. }
				| ValueType::Any
				| ValueType::List(_)
				| ValueType::Record(_)
				| ValueType::Tuple(_)
		) {
			for (key, row) in keys.iter_mut().zip(rows) {
				let value = shape.get_value(row, index);
				key.extend_bytes(
					to_stdvec(&value).expect("postcard serialization of a Value is total"),
				);
			}
		} else {
			let mut builder = ColumnBuilder::with_capacity(field_type, rows.len());
			decode_cells(&mut builder, &field.name, shape, index, rows)?;
			let column = builder.finish(&field.name);
			extend_keys(&ColumnView::try_from(&column)?, &mut keys)?;
		}
	}
	Ok(keys.into_iter().map(|key| key.finish().to_vec()).collect())
}

#[inline]
fn truncate_to_declared(columns: RecordBatch, declared: usize) -> Result<RecordBatch> {
	let kept: Vec<(FieldRef, ArrayRef)> =
		user_columns(&columns).take(declared).map(|(field, array)| (field.clone(), array.clone())).collect();
	rebuild_with_user_columns(&columns, kept)
}

fn rebuild_with_user_columns(source: &RecordBatch, user: Vec<(FieldRef, ArrayRef)>) -> Result<RecordBatch> {
	let mut out = batch(user)?;
	for column in SystemColumn::ALL {
		if let Some(array) = system_column(source, column) {
			out = with_system_column(out, column, array.clone())?;
		}
	}
	Ok(out)
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
	identity: IdentityId,
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
		identity,
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
	fragments: &InputFragments,
	has_deduplication: bool,
	has_not_before: bool,
) -> Result<Vec<PendingItem>> {
	let mut mutable_context = (**context).clone();
	let declared_key_indices = declared_key_indices(target.queue)?;
	let pipeline = ColumnPipeline {
		columns: &target.queue.columns,
		sequences: Some(target.queue.id.into()),
		series_key: None,
		fragments,
		context,
	};
	let mut casts: Vec<CastColumns> = Vec::new();
	let mut not_befores: Vec<Vec<Option<DateTime>>> = Vec::new();
	let mut statement_keys: Vec<Option<Vec<Vec<u8>>>> = Vec::new();

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.queue.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Queue,
		)?;
		if let Some((unknown, _)) = user_columns(&columns).find(|(field, _)| {
			!(target.queue.columns.iter().any(|c| &c.name == field.name())
				|| (has_deduplication && field.name() == QUEUE_DEDUPLICATION_KEY_FIELD)
				|| (has_not_before && field.name() == QUEUE_NOT_BEFORE_FIELD))
		}) {
			return_error!(column_not_found(fragments.column(unknown.name())));
		}

		let rows = columns.num_rows();
		let not_before_view = match has_not_before {
			true => named_view(&columns, QUEUE_NOT_BEFORE_FIELD)?,
			false => None,
		};
		let (not_before, not_before_failure) = read_not_before(target, not_before_view.as_ref(), rows);
		let key_view = match declared_key_indices.is_none() && has_deduplication {
			true => named_view(&columns, QUEUE_DEDUPLICATION_KEY_FIELD)?,
			false => None,
		};
		let key_failure = key_view.as_ref().and_then(|view| statement_key_failure(target, &pipeline, view));

		let inputs = input_views(&columns, &target.queue.columns)?;
		let earliest = Failure::earliest([not_before_failure, key_failure]);
		let mut cast = pipeline.cast_target_columns(&inputs, rows, earliest)?;
		pipeline.fill_sequences(services, txn, &mut cast)?;

		statement_keys.push(key_view.as_ref().map(statement_key_bytes).transpose()?);
		not_befores.push(not_before);
		casts.push(cast);
	}
	intern_dictionary_columns(&services.catalog, txn, pipeline.columns, pipeline.series_key, &mut casts)?;

	let populator = populator_index(&target.queue.time, shape);
	let mut pending: Vec<PendingItem> = Vec::new();
	for ((cast, not_before), statement_keys) in casts.iter().zip(not_befores).zip(statement_keys) {
		let mut rows: Vec<EncodedQueueRowBuilder> = (0..cast.rows()).map(|_| shape.allocate_queue()).collect();
		cast.write(shape, &mut rows)?;
		let now = services.runtime_context.clock.now();
		let event = EventColumn::new(populator.map(|index| cast.view(index)).transpose()?);
		let mut encoded = Vec::with_capacity(rows.len());
		for (index, (mut row, not_before)) in rows.into_iter().zip(&not_before).enumerate() {
			if let Some(instant) = not_before {
				row.set_not_before(*instant);
			}
			row.set_timestamps(now, now);
			if let Some(time) = event.at(index).map_or_else(
				|| resolve_time(&target.queue.name, &target.queue.time, shape, &row, now),
				|time| Ok(Some(time)),
			)? {
				row.set_time(time);
			}
			encoded.push(row.freeze_bytes());
		}

		let keys: Vec<Option<Vec<u8>>> = match (&declared_key_indices, statement_keys) {
			(Some(indices), _) => declared_keys(shape, &encoded, indices)?.into_iter().map(Some).collect(),
			(None, Some(keys)) => keys.into_iter().map(Some).collect(),
			(None, None) => vec![None; encoded.len()],
		};
		for ((encoded, deduplication_key), not_before) in encoded.into_iter().zip(keys).zip(not_before) {
			pending.push(PendingItem {
				encoded,
				deduplication_key,
				not_before,
			});
		}
	}

	Ok(pending)
}

fn named_view<'a>(columns: &'a RecordBatch, name: &str) -> Result<Option<ColumnView<'a>>> {
	user_columns(columns)
		.filter(|(field, _)| field.name() == name)
		.last()
		.map(|(field, array)| ColumnView::try_from((array, field.as_ref())))
		.transpose()
}

fn statement_key_failure(
	target: &QueueTarget<'_>,
	pipeline: &ColumnPipeline<'_>,
	view: &ColumnView<'_>,
) -> Option<Failure> {
	if matches!(view.data, ViewData::Utf8 { .. } | ViewData::None { .. }) {
		return None;
	}
	(0..view.len()).find_map(|row| match view.get_value(row) {
		Value::None {
			..
		}
		| Value::Utf8(_) => None,
		other => Some(Failure::after_columns(
			pipeline,
			row,
			Error(Box::new(queue_deduplication_key_not_utf8(
				Fragment::internal(target.queue.name.clone()),
				other.get_type().to_string().as_str(),
			))),
		)),
	})
}

fn statement_key_bytes(view: &ColumnView<'_>) -> Result<Vec<Vec<u8>>> {
	let mut keys: Vec<KeySerializer> = (0..view.len()).map(|_| KeySerializer::new()).collect();
	if matches!(
		view.base_type(),
		ValueType::Digest { .. } | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_)
	) {
		for (row, key) in keys.iter_mut().enumerate() {
			key.extend_bytes(
				to_stdvec(&view.get_value(row)).expect("postcard serialization of a Value is total"),
			);
		}
	} else {
		extend_keys(view, &mut keys)?;
	}
	Ok(keys.into_iter().map(|key| key.finish().to_vec()).collect())
}

fn read_not_before(
	target: &QueueTarget<'_>,
	view: Option<&ColumnView<'_>>,
	rows: usize,
) -> (Vec<Option<DateTime>>, Option<Failure>) {
	let Some(view) = view else {
		return (vec![None; rows], None);
	};
	let mut values = Vec::with_capacity(rows);
	for row in 0..rows {
		match view.get_value(row) {
			Value::None {
				..
			} => values.push(None),
			Value::DateTime(instant) => values.push(Some(instant)),
			other => {
				let error = Error(Box::new(queue_not_before_not_datetime(
					Fragment::internal(target.queue.name.clone()),
					other.get_type().to_string().as_str(),
				)));
				values.resize(rows, None);
				return (values, Some(Failure::before_columns(row, error)));
			}
		}
	}
	(values, None)
}

#[inline]
fn insert_queue_result(namespace: &str, queue: &str, inserted: u64, duplicates: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("queue", Value::Utf8(queue.to_string())),
		("inserted", Value::Uint8(inserted)),
		("duplicates", Value::Uint8(duplicates)),
	])
}
