// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::reifydb_assertions;
use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		index::primary_key_violation,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::IndexId,
			key::PrimaryKey,
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			table::Table,
		},
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedObject, ResolvedTable},
	},
	internal_error,
	key::{EncodableKey, index_entry::IndexEntryKey},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::InsertTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, partition::Partition, row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use super::{
	context::TableTarget,
	primary_key,
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_table_shape,
};
use crate::{
	Result,
	partition::{partition_col_indices, partition_values, resolve_partition},
	policy::PolicyEvaluator,
	transaction::operation::{dictionary::DictionaryOperations, table::TableOperations},
	vm::{
		instruction::dml::coerce::coerce_value_to_column_type,
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

#[instrument(name = "mutate::table::insert", level = "trace", skip_all)]
pub(crate) fn insert_table(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertTableNode,
	symbols: &mut SymbolTable,
) -> Result<Columns> {
	let InsertTableNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, table) = resolve_insert_table_target(services, txn, &target)?;
	let shape = get_or_create_table_shape(&services.catalog, &table, txn)?;
	let target_data = TableTarget {
		namespace: &namespace,
		table: &table,
		fragment: target.identifier(),
	};
	let context = build_insert_table_query_context(services, &target_data, symbols);
	let mut input_node = compile(*input, txn, context.clone());
	input_node.initialize(txn, &context)?;

	let validated_rows = validate_and_encode_input_rows(
		services,
		txn,
		&target_data,
		&shape,
		&context,
		symbols,
		&mut input_node,
	)?;

	if !table.partition_by.is_empty() {
		let indices = partition_col_indices(&table.columns, &table.partition_by);
		let mut verified = HashSet::new();
		for row in &validated_rows {
			let values = partition_values(&shape, row, &indices);
			let partition = Partition::of(&values);
			resolve_partition(txn, ObjectId::Table(table.id), partition, &values, &mut verified)?;
		}
	}

	let total_rows = validated_rows.len();
	if total_rows == 0 {
		return Ok(insert_table_result(namespace.name(), &table.name, 0));
	}

	let row_numbers = services.catalog.next_row_number_batch(txn, table.id, total_rows as u64)?;
	assert_eq!(row_numbers.len(), validated_rows.len());

	let pk_def = primary_key::get_primary_key(&services.catalog, txn, &table)?;
	let row_number_shape = pk_def.as_ref().map(|_| RowShape::testing(&[ValueType::Uint8]));
	let pk_ctx = pk_def.as_ref().map(|pk| PkContext {
		pk_def: pk,
		row_number_shape: row_number_shape.as_ref().unwrap(),
	});
	let returned_rows = insert_validated_table_rows(
		txn,
		&target_data,
		&shape,
		&validated_rows,
		&row_numbers,
		returning.is_some(),
		pk_ctx.as_ref(),
	)?;

	if let Some(returning_exprs) = &returning {
		let mut columns = decode_rows_to_columns(&shape, &returned_rows);
		decode_returning_dictionaries(services, txn, &table.columns, &mut columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(insert_table_result(namespace.name(), &table.name, total_rows as u64))
}

struct PkContext<'a> {
	pk_def: &'a PrimaryKey,
	row_number_shape: &'a RowShape,
}

struct ColumnView<'a> {
	columns: &'a Columns,
	column_map: &'a HashMap<&'a str, usize>,
}

#[inline]
fn resolve_insert_table_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedTable,
) -> Result<(Namespace, Table)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let table_name = target.name();
	let Some(table) = services.catalog.find_table_by_name(txn, namespace.id(), table_name)? else {
		let fragment = target.identifier().clone();
		return_error!(table_not_found(fragment.clone(), namespace_name, table_name,));
	};
	Ok((namespace, table))
}

#[inline]
fn build_insert_table_query_context(
	services: &Arc<Services>,
	target: &TableTarget<'_>,
	symbols: &SymbolTable,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let table_ident = Fragment::internal(target.table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, target.table.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::Table(resolved_table)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: Params::None,
		symbols: symbols.clone(),
		identity: IdentityId::root(),
		memory: query_budget(services),
	})
}

fn validate_and_encode_input_rows(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	context: &Arc<QueryContext>,
	symbols: &SymbolTable,
	input_node: &mut Box<dyn QueryNode>,
) -> Result<Vec<EncodedRow>> {
	let mut validated_rows: Vec<EncodedRow> = Vec::new();
	let mut mutable_context = (**context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.table.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Table,
		)?;
		let mut column_map: HashMap<&str, usize> = HashMap::new();
		for (idx, col) in columns.iter().enumerate() {
			column_map.insert(col.name().text(), idx);
		}
		let view = ColumnView {
			columns: &columns,
			column_map: &column_map,
		};
		let row_count = columns.row_count();
		for row_idx in 0..row_count {
			validated_rows
				.push(build_insert_table_row(services, txn, target, shape, &view, context, row_idx)?);
		}
	}
	Ok(validated_rows)
}

#[inline]
fn build_insert_table_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	view: &ColumnView<'_>,
	context: &Arc<QueryContext>,
	row_idx: usize,
) -> Result<EncodedRow> {
	let mut row = shape.allocate();
	for (table_idx, table_column) in target.table.columns.iter().enumerate() {
		let mut value = if let Some(&input_idx) = view.column_map.get(table_column.name.as_str()) {
			view.columns[input_idx].get_value(row_idx)
		} else {
			Value::none()
		};
		if table_column.auto_increment && matches!(value, Value::None { .. }) {
			value = services.catalog.column_sequence_next_value(txn, target.table.id, table_column.id)?;
		}
		let column_ident = view
			.column_map
			.get(table_column.name.as_str())
			.map(|&idx| view.columns.name_at(idx).clone())
			.unwrap_or_else(|| Fragment::internal(table_column.name.clone()));
		let resolved_column = ResolvedColumn::new(
			column_ident.clone(),
			context.source.clone().unwrap(),
			table_column.clone(),
		);
		value = coerce_value_to_column_type(
			value,
			table_column.constraint.get_type(),
			resolved_column,
			context,
		)?;
		if let Err(mut e) = table_column.constraint.validate(&value) {
			e.0.fragment = column_ident.clone();
			return Err(e);
		}
		let value = if let Some(dict_id) = table_column.dictionary_id {
			let dictionary = services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
				internal_error!("Dictionary {:?} not found for column {}", dict_id, table_column.name)
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
		shape.set_value(&mut row, table_idx, &value);
	}
	let now_nanos = services.runtime_context.clock.now_nanos();
	row.set_timestamps(now_nanos, now_nanos);
	row.set_time_nanos(resolve_row_time_nanos(&target.table, shape, &row, now_nanos));
	Ok(row)
}

pub(crate) fn resolve_row_time_nanos(table: &Table, shape: &RowShape, row: &EncodedRow, arrival_nanos: u64) -> u64 {
	let Some(ts_column) = table.time.ts() else {
		return arrival_nanos;
	};

	let index = table.columns.iter().position(|c| c.name == ts_column);

	reifydb_assertions! {
		assert!(
			index.is_some(),
			"{}.{ts_column} is the declared #time populator but is absent from the table's own \
			 columns; definition-time validation must reject that, so reaching here means a table was \
			 stored with a populator naming a column it does not have",
			table.name
		);
	}

	let Some(index) = index else {
		return arrival_nanos;
	};

	match shape.get_value(row, index) {
		Value::DateTime(dt) => dt.to_nanos(),
		other => {
			reifydb_assertions! {
				assert!(
					false,
					"{}.{ts_column} is the declared #time populator and must be a non-none \
					 DateTime on every row; definition-time validation rejects none-able and \
					 non-DateTime populators, so a {other:?} here means a row bypassed that check",
					table.name
				);
			}
			arrival_nanos
		}
	}
}

fn insert_validated_table_rows(
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	validated_rows: &[EncodedRow],
	row_numbers: &[RowNumber],
	has_returning: bool,
	pk: Option<&PkContext<'_>>,
) -> Result<Vec<(RowNumber, EncodedRow)>> {
	let mut owned_rows: Vec<EncodedRow> = validated_rows.to_vec();
	txn.insert_table(target.table, shape, row_numbers, &mut owned_rows)?;

	if let Some(pk) = pk {
		for (row, &row_number) in owned_rows.iter().zip(row_numbers.iter()) {
			write_insert_table_pk_index(txn, target, shape, pk, row, row_number)?;
		}
	}

	if has_returning {
		Ok(row_numbers.iter().copied().zip(owned_rows).collect())
	} else {
		Ok(Vec::new())
	}
}

#[inline]
fn write_insert_table_pk_index(
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	pk: &PkContext<'_>,
	row: &EncodedRow,
	row_number: RowNumber,
) -> Result<()> {
	let index_key = primary_key::encode_primary_key(pk.pk_def, row, target.table, shape)?;
	let index_entry_key = IndexEntryKey::new(target.table.id, IndexId::primary(pk.pk_def.id), index_key.clone());
	if txn.contains_key(&index_entry_key.encode())? {
		let key_columns = pk.pk_def.columns.iter().map(|c| c.name.clone()).collect();
		return_error!(primary_key_violation(target.fragment.clone(), target.table.name.clone(), key_columns,));
	}
	let mut row_number_encoded = pk.row_number_shape.allocate();
	pk.row_number_shape.set_u64(&mut row_number_encoded, 0, u64::from(row_number));
	txn.set(&index_entry_key.encode(), row_number_encoded)?;
	Ok(())
}

#[inline]
fn insert_table_result(namespace: &str, table: &str, inserted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}

#[cfg(test)]
mod time_population_tests {
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			column::{Column, ColumnIndex},
			id::{ColumnId, NamespaceId, TableId},
		},
	};
	use reifydb_value::value::{
		constraint::TypeConstraint, datetime::DateTime, value_type::ValueType,
	};

	use super::*;

	const ARRIVAL: u64 = 1_900_000_000_000_000_000;
	const BLOCK_TIME: u64 = 1_700_000_000_000_000_000;

	fn column(name: &str, ty: ValueType, index: u8) -> Column {
		Column {
			id: ColumnId(index as u64 + 1),
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: vec![],
			index: ColumnIndex(index),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	fn table(time: TimeSource) -> Table {
		Table {
			id: TableId(1),
			namespace: NamespaceId(1),
			name: "trades".to_string(),
			columns: vec![
				column("signature", ValueType::Utf8, 0),
				column("block_time", ValueType::DateTime, 1),
			],
			primary_key: None,
			partition_by: vec![],
			underlying: false,
			time,
		}
	}

	fn shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("signature", ValueType::Utf8),
			RowShapeField::unconstrained("block_time", ValueType::DateTime),
		])
	}

	fn row(shape: &RowShape, block_time_nanos: u64) -> EncodedRow {
		let mut row = shape.allocate();
		shape.set_value(&mut row, 0, &Value::Utf8("sig".to_string()));
		shape.set_value(&mut row, 1, &Value::DateTime(DateTime::from_nanos(block_time_nanos)));
		row
	}

	#[test]
	// Intent: an event-time table stamps #time from the column the author declared, not from the
	// clock. This is the property the whole redesign rests on - it is what makes a replay of an
	// old corpus reproduce production's retention decisions instead of re-dating every row to now.
	// Mutation: return arrival_nanos unconditionally and this fails with the wall clock.
	fn an_event_time_table_stamps_time_from_the_declared_populator() {
		let shape = shape();
		let table = table(TimeSource::Event {
			ts: "block_time".to_string(),
		});

		let stamped = resolve_row_time_nanos(&table, &shape, &row(&shape, BLOCK_TIME), ARRIVAL);

		assert_eq!(stamped, BLOCK_TIME, "#time must come from block_time, not from the write clock");
	}

	#[test]
	// Intent: a table that declares nothing is processing-time, and its #time is arrival. Silence
	// is a legitimate declaration and must not leave #time unset - D1 says a row without a time is
	// unrepresentable.
	fn a_processing_time_table_stamps_time_from_arrival() {
		let shape = shape();
		let table = table(TimeSource::Processing);

		let stamped = resolve_row_time_nanos(&table, &shape, &row(&shape, BLOCK_TIME), ARRIVAL);

		assert_eq!(stamped, ARRIVAL);
	}

	#[test]
	// Intent: the replay property in miniature. When the populator value is OLDER than the write,
	// #time and the wall stamps must diverge - #time says when the event happened, created_at says
	// when this database learned about it. A backfill of week-old data must land at its own event
	// time, or every windowed rollup over it buckets into today.
	// Mutation: populate #time from the wall clock and the two collapse onto each other here.
	fn time_diverges_from_arrival_when_the_event_predates_the_write() {
		let shape = shape();
		let table = table(TimeSource::Event {
			ts: "block_time".to_string(),
		});

		let stamped = resolve_row_time_nanos(&table, &shape, &row(&shape, BLOCK_TIME), ARRIVAL);

		assert!(stamped < ARRIVAL, "a backfilled row's #time must predate its arrival");
		assert_eq!(ARRIVAL - stamped, 200_000_000_000_000_000);
	}

	#[test]
	// Intent: the populator is resolved by NAME against the table's own columns, so it keeps
	// working when the declared column is not the last one and when other columns share its type.
	// Mutation: hardcode the last column index and this returns the wrong column's value.
	fn the_populator_is_resolved_by_name_not_by_position() {
		let shape = RowShape::new(vec![
			RowShapeField::unconstrained("block_time", ValueType::DateTime),
			RowShapeField::unconstrained("recorded_at", ValueType::DateTime),
		]);
		let mut table = table(TimeSource::Event {
			ts: "block_time".to_string(),
		});
		table.columns = vec![
			column("block_time", ValueType::DateTime, 0),
			column("recorded_at", ValueType::DateTime, 1),
		];

		let mut r = shape.allocate();
		shape.set_value(&mut r, 0, &Value::DateTime(DateTime::from_nanos(BLOCK_TIME)));
		shape.set_value(&mut r, 1, &Value::DateTime(DateTime::from_nanos(ARRIVAL)));

		assert_eq!(resolve_row_time_nanos(&table, &shape, &r, 0), BLOCK_TIME);
	}
}
