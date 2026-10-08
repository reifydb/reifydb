// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_catalog::{
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_codec::row::{
	bytes::{RowBuilder, SourceRowBuilder},
	shape::RowShape,
};
use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		column::Column,
		namespace::Namespace,
		object::ObjectId,
		policy::{DataOp, PolicyTargetType},
		ringbuffer::RingBuffer,
		series::{Series, SeriesPartitionMetadata},
		table::Table,
	},
	internal_error,
	value::batch::batch,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_runtime::context::clock::Clock;
#[cfg(feature = "testing")]
use reifydb_transaction::counting::OpCounts;
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	value::{
		Value, column_view::ColumnView, constraint::Constraint, datetime::DateTime, identity::IdentityId,
		partition::Partition, value_type::ValueType,
	},
};

use super::{
	BulkInsertResult, RingBufferInsertResult, SeriesInsertResult, TableInsertResult,
	coerce::with_row_note,
	validation::{CoercedTags, coerce_columns},
};
use crate::{
	Result,
	bulk_insert::storage::{
		ringbuffer::{PendingRingBufferInsert, RingBufferInsertBuilder},
		series::{PendingSeriesInsert, SeriesInsertBuilder},
		table::{PendingTableInsert, TableInsertBuilder},
	},
	engine::StandardEngine,
	error::EngineError,
	partition::resolve_partitions,
	policy::PolicyEvaluator,
	transaction::operation::series::{SeriesRows, write_series_rows},
	vm::{
		instruction::dml::{
			coerce::key_out_of_range,
			columns::{CastColumns, fill_sequences, intern_dictionary_columns},
			partition::{ringbuffer_batch_partitions, save_all_partition_metadata},
			primary_key::Violation,
			ringbuffer_insert::insert_ringbuffer_chunks,
			series_insert::{assign_series_keys, series_batch_partitions},
			shape::{
				get_or_create_ringbuffer_shape, get_or_create_series_shape, get_or_create_table_shape,
			},
			table_insert::{EncodedTableRows, table_batch_partitions, write_table_rows},
			time::{EventColumn, populator_index, resolve_time},
		},
		services::Services,
	},
};

pub trait ValidationMode: sealed::Sealed + 'static {
	const VALIDATED: bool;

	fn run<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>;
}

pub struct Validated;
impl ValidationMode for Validated {
	const VALIDATED: bool = true;

	fn run<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>,
	{
		run_checked(txn, total_rows, body)
	}
}

pub struct Unchecked;
impl ValidationMode for Unchecked {
	const VALIDATED: bool = false;

	fn run<F, R>(txn: &mut CommandTransaction, _total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>,
	{
		txn.execute_bulk_unchecked(body)
	}
}

fn run_checked<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
where
	F: FnOnce(&mut CommandTransaction) -> Result<R>,
{
	if total_rows > 0 {
		txn.reserve_writes(total_rows.saturating_mul(2))?;
	}
	let r = body(txn)?;
	txn.commit()?;
	Ok(r)
}

pub mod sealed {

	use super::{Unchecked, Validated};
	pub trait Sealed {}
	impl Sealed for Validated {}
	impl Sealed for Unchecked {}
}

pub struct BulkInsertBuilder<'e, V: ValidationMode = Validated> {
	engine: &'e StandardEngine,
	identity: IdentityId,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
	pending_series: Vec<PendingSeriesInsert>,
	_validation: PhantomData<V>,
}

impl<'e> BulkInsertBuilder<'e, Validated> {
	pub(crate) fn new(engine: &'e StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			pending_series: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e> BulkInsertBuilder<'e, Unchecked> {
	pub(crate) fn new_unchecked(engine: &'e StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			pending_series: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e, V: ValidationMode> BulkInsertBuilder<'e, V> {
	pub fn table<'a>(&'a mut self, qualified_name: &str) -> TableInsertBuilder<'a, 'e, V> {
		let (namespace, table) = parse_qualified_name(qualified_name);
		TableInsertBuilder::new(self, namespace, table)
	}

	pub fn ringbuffer<'a>(&'a mut self, qualified_name: &str) -> RingBufferInsertBuilder<'a, 'e, V> {
		let (namespace, ringbuffer) = parse_qualified_name(qualified_name);
		RingBufferInsertBuilder::new(self, namespace, ringbuffer)
	}

	pub fn series<'a>(&'a mut self, qualified_name: &str) -> SeriesInsertBuilder<'a, 'e, V> {
		let (namespace, series) = parse_qualified_name(qualified_name);
		SeriesInsertBuilder::new(self, namespace, series)
	}

	pub(super) fn add_table_insert(&mut self, pending: PendingTableInsert) {
		self.pending_tables.push(pending);
	}

	pub(super) fn add_ringbuffer_insert(&mut self, pending: PendingRingBufferInsert) {
		self.pending_ringbuffers.push(pending);
	}

	pub(super) fn add_series_insert(&mut self, pending: PendingSeriesInsert) {
		self.pending_series.push(pending);
	}

	pub fn execute(self) -> Result<BulkInsertResult> {
		self.run(|_| ()).map(|(result, ())| result)
	}

	#[cfg(feature = "testing")]
	pub fn execute_counted(self) -> Result<(BulkInsertResult, OpCounts)> {
		self.run(CommandTransaction::op_counts)
	}

	fn run<T>(self, observe: impl FnOnce(&CommandTransaction) -> T) -> Result<(BulkInsertResult, T)> {
		self.engine.reject_if_read_only()?;
		let mut txn = self.engine.begin_command(self.identity)?;
		let catalog = self.engine.catalog();
		let services = self.engine.services();
		let clock = self.engine.clock();
		let total_rows = self.total_pending_rows();
		let pending_tables = self.pending_tables;
		let pending_ringbuffers = self.pending_ringbuffers;
		let pending_series = self.pending_series;

		V::run(&mut txn, total_rows, move |txn| {
			let writer = Writer {
				catalog: &catalog,
				services: &services,
				clock,
			};
			let result = run_all_pending::<V>(
				&writer,
				txn,
				pending_tables,
				pending_ringbuffers,
				pending_series,
			)?;
			Ok((result, observe(txn)))
		})
	}

	#[inline]
	fn total_pending_rows(&self) -> usize {
		self.pending_tables.iter().map(|p| p.row_count()).sum::<usize>()
			+ self.pending_ringbuffers.iter().map(|p| p.row_count()).sum::<usize>()
			+ self.pending_series.iter().map(|p| p.row_count()).sum::<usize>()
	}
}

struct Writer<'a> {
	catalog: &'a Catalog,
	services: &'a Arc<Services>,
	clock: &'a Clock,
}

#[inline]
fn run_all_pending<V: ValidationMode>(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
	pending_series: Vec<PendingSeriesInsert>,
) -> Result<BulkInsertResult> {
	let mut result = BulkInsertResult::default();
	for pending in pending_tables {
		result.tables.push(execute_table_insert::<V>(writer, txn, &pending)?);
	}
	for pending in pending_ringbuffers {
		result.ringbuffers.push(execute_ringbuffer_insert::<V>(writer, txn, &pending)?);
	}
	for pending in pending_series {
		result.series.push(execute_series_insert::<V>(writer, txn, &pending)?);
	}
	Ok(result)
}

struct Checks<'a> {
	name: &'a str,
	columns: &'a [Column],
	filled: &'a [Vec<usize>],
	series: Option<&'a Series>,
	time: &'a TimeSource,
}

struct Checked {
	keys: Vec<u64>,
	tags: Vec<Option<u8>>,
}

fn check_rows<V: ValidationMode>(
	checks: &Checks<'_>,
	columns: &[(FieldRef, ArrayRef)],
	tags: Option<CoercedTags>,
	rows: usize,
) -> Result<Checked> {
	let views = columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
	let mut failure: Option<(usize, Error)> = None;
	let limit = |failure: &Option<(usize, Error)>| failure.as_ref().map_or(rows, |(row, _)| *row);

	if V::VALIDATED {
		for ((column, view), filled) in checks.columns.iter().zip(&views).zip(checks.filled) {
			if let Some(row) = first_constraint_failure(column, view, filled, limit(&failure)) {
				failure = Some((row, constraint_error(column, view.get_value(row), checks.name, row)));
			}
		}
	}

	let mut keys = Vec::new();
	if let Some(series) = checks.series {
		let index = key_column_index(series)?;
		let (column, view) = (&checks.columns[index], &views[index]);
		let given = series.key.keys_to_u64(view);
		let dictionary_key = column.dictionary_id.is_some();
		for (row, key) in given.iter().enumerate().take(limit(&failure)) {
			if view.none_at(row) {
				failure = Some((row, constraint_error(column, view.get_value(row), checks.name, row)));
				break;
			}
			if dictionary_key || key.is_none() {
				failure = Some((row, key_out_of_range(&series.key, &view.get_value(row))));
				break;
			}
		}
		if failure.is_none() {
			keys = given.into_iter().collect::<Option<Vec<u64>>>().ok_or_else(|| {
				internal_error!("bulk insert into series {} kept a row without a key", series.name)
			})?;
		}
	}

	let mut variant_tags = vec![None; rows];
	if let Some((tags, tag_failure)) = tags {
		variant_tags = tags;
		if let Some((row, error)) = tag_failure
			&& row < limit(&failure)
		{
			failure = Some((row, error));
		}
	}

	if let TimeSource::Event {
		ts,
	} = checks.time && let Some(index) = checks.columns.iter().position(|column| &column.name == ts)
		&& checks.columns[index].dictionary_id.is_none()
	{
		let view = &views[index];
		if let Some(row) = (0..limit(&failure)).find(|&row| view.none_at(row)) {
			failure = Some((
				row,
				EngineError::TimePopulatorNotDateTime {
					object: checks.name.to_string(),
					column: ts.to_string(),
					found: format!("{:?}", Value::none()),
				}
				.into(),
			));
		}
	}

	if let Some((_, error)) = failure {
		return Err(error);
	}
	Ok(Checked {
		keys,
		tags: variant_tags,
	})
}

fn first_constraint_failure(column: &Column, view: &ColumnView<'_>, filled: &[usize], limit: usize) -> Option<usize> {
	let target = column.constraint.get_type();
	let max_bytes = match column.constraint.constraint() {
		Some(Constraint::MaxBytes(max)) => Some(usize::from(*max)),
		_ => None,
	};
	(0..limit).find(|&row| {
		if view.none_at(row) {
			return !target.is_option() && filled.binary_search(&row).is_err();
		}
		match (target.inner_type(), max_bytes) {
			(ValueType::Utf8, Some(max)) => view.get_str(row).is_some_and(|text| text.len() > max),
			(ValueType::Blob, Some(max)) => view.get_bytes(row).is_some_and(|bytes| bytes.len() > max),
			_ => false,
		}
	})
}

fn constraint_error(column: &Column, mut value: Value, name: &str, row: usize) -> Error {
	let mut error = match column.constraint.coerce(&mut value) {
		Err(error) => error,
		Ok(()) => internal_error!("bulk column {} passed the constraint its scan refused", column.name),
	};
	error.0.fragment = Fragment::internal(&column.name);
	with_row_note(error, name, row)
}

fn key_column_index(series: &Series) -> Result<usize> {
	let key_col_name = series.key.column();
	series.columns
		.iter()
		.position(|c| c.name == key_col_name)
		.ok_or_else(|| internal_error!("series {} key column {} not found", series.name, key_col_name))
}

fn enforce_write_policies(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	namespace: &Namespace,
	object: &str,
	target: PolicyTargetType,
	columns: &[(FieldRef, ArrayRef)],
) -> Result<()> {
	let symbols = SymbolTable::new();
	PolicyEvaluator::new(writer.services, &symbols).enforce_write_policies(
		&mut Transaction::Command(txn),
		namespace.name(),
		object,
		DataOp::Insert,
		&batch(columns.to_vec())?,
		target,
	)
}

fn stamp<B: SourceRowBuilder>(
	row: &mut B,
	name: &str,
	time: &TimeSource,
	shape: &RowShape,
	now: DateTime,
	event: Option<DateTime>,
) -> Result<()> {
	row.set_timestamps(now, now);
	if let Some(time) =
		event.map_or_else(|| resolve_time(name, time, shape, row.as_slice(), now), |time| Ok(Some(time)))?
	{
		row.set_time(time);
	}
	Ok(())
}

fn execute_table_insert<V: ValidationMode>(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	pending: &PendingTableInsert,
) -> Result<TableInsertResult> {
	let catalog = writer.catalog;
	let (namespace, table) = resolve_table(catalog, txn, pending)?;
	let shape = get_or_create_table_shape(catalog, &table, &mut Transaction::Command(txn))?;
	let (columns, _) =
		coerce_columns(&pending.rows, &pending.batches, &table.columns, None, &table.name, txn.identity)?;
	let rows = pending.row_count();
	if rows == 0 {
		return Ok(empty_table_result(pending));
	}
	enforce_write_policies(writer, txn, &namespace, &table.name, PolicyTargetType::Table, &columns)?;

	let filled = auto_increment_rows(&table.columns, &columns)?;
	check_rows::<V>(
		&Checks {
			name: &table.name,
			columns: &table.columns,
			filled: &filled,
			series: None,
			time: &table.time,
		},
		&columns,
		None,
		rows,
	)?;
	let fills = filled.into_iter().enumerate().filter(|(_, rows)| !rows.is_empty()).collect();
	let mut cast = [CastColumns::with_fills(columns, fills, rows)];
	intern_dictionary_columns(catalog, &mut Transaction::Command(txn), &table.columns, None, &mut cast)?;
	fill_sequences(catalog, &mut Transaction::Command(txn), &table.columns, table.id.into(), &mut cast)?;
	let mut encoded = EncodedTableRows {
		rows: (0..rows).map(|_| shape.allocate_table()).collect(),
		..EncodedTableRows::default()
	};
	cast[0].write(&shape, &mut encoded.rows)?;
	let now = writer.clock.now();
	let event =
		EventColumn::new(populator_index(&table.time, &shape).map(|index| cast[0].view(index)).transpose()?);
	for (index, row) in encoded.rows.iter_mut().enumerate() {
		stamp(row, &table.name, &table.time, &shape, now, event.at(index))?;
	}
	table_batch_partitions(&table, &cast[0], &mut encoded)?;
	write_table_rows(
		catalog,
		&mut Transaction::Command(txn),
		&table,
		&shape,
		&mut encoded,
		&Violation::BulkRow(&table.name),
	)?;
	Ok(TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: rows as u64,
	})
}

fn auto_increment_rows(columns: &[Column], cast: &[(FieldRef, ArrayRef)]) -> Result<Vec<Vec<usize>>> {
	columns.iter()
		.zip(cast)
		.map(|(column, cast)| {
			if !column.auto_increment {
				return Ok(Vec::new());
			}
			let view = ColumnView::try_from(cast)?;
			Ok((0..view.len()).filter(|&row| view.none_at(row)).collect())
		})
		.collect()
}

#[inline]
fn empty_table_result(pending: &PendingTableInsert) -> TableInsertResult {
	TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: 0,
	}
}

fn resolve_namespace(catalog: &Catalog, txn: &mut CommandTransaction, name: &str) -> Result<Namespace> {
	catalog.find_namespace_by_name(&mut Transaction::Command(txn), name)?.ok_or_else(|| {
		CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: name.to_string(),
			name: String::new(),
			fragment: Fragment::None,
		}
		.into()
	})
}

fn resolve_table(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingTableInsert,
) -> Result<(Namespace, Table)> {
	let namespace = resolve_namespace(catalog, txn, &pending.namespace)?;
	let table = catalog
		.find_table_by_name(&mut Transaction::Command(txn), namespace.id(), &pending.table)?
		.ok_or_else(|| -> Error {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Table,
				namespace: pending.namespace.to_string(),
				name: pending.table.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})?;
	Ok((namespace, table))
}

fn execute_ringbuffer_insert<V: ValidationMode>(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	pending: &PendingRingBufferInsert,
) -> Result<RingBufferInsertResult> {
	let catalog = writer.catalog;
	let (namespace, ringbuffer) = resolve_ringbuffer(catalog, txn, pending)?;
	let shape = get_or_create_ringbuffer_shape(catalog, &ringbuffer, &mut Transaction::Command(txn))?;
	let (columns, _) = coerce_columns(
		&pending.rows,
		&pending.batches,
		&ringbuffer.columns,
		None,
		&ringbuffer.name,
		txn.identity,
	)?;
	let rows = pending.row_count();
	let inserted = if rows == 0 {
		0
	} else {
		insert_ringbuffer_rows::<V>(writer, txn, &namespace, &ringbuffer, &shape, columns, rows)?
	};
	Ok(RingBufferInsertResult {
		namespace: pending.namespace.clone(),
		ringbuffer: pending.ringbuffer.clone(),
		inserted,
	})
}

#[inline]
fn resolve_ringbuffer(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingRingBufferInsert,
) -> Result<(Namespace, RingBuffer)> {
	let namespace = resolve_namespace(catalog, txn, &pending.namespace)?;
	let ringbuffer = catalog
		.find_ringbuffer_by_name(&mut Transaction::Command(txn), namespace.id(), &pending.ringbuffer)?
		.ok_or_else(|| -> Error {
			CatalogError::NotFound {
				kind: CatalogObjectKind::RingBuffer,
				namespace: pending.namespace.to_string(),
				name: pending.ringbuffer.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})?;
	Ok((namespace, ringbuffer))
}

fn insert_ringbuffer_rows<V: ValidationMode>(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	namespace: &Namespace,
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	columns: Vec<(FieldRef, ArrayRef)>,
	rows: usize,
) -> Result<u64> {
	let catalog = writer.catalog;
	enforce_write_policies(writer, txn, namespace, &ringbuffer.name, PolicyTargetType::RingBuffer, &columns)?;
	check_rows::<V>(
		&Checks {
			name: &ringbuffer.name,
			columns: &ringbuffer.columns,
			filled: &vec![Vec::new(); ringbuffer.columns.len()],
			series: None,
			time: &ringbuffer.time,
		},
		&columns,
		None,
		rows,
	)?;

	let mut cast = [CastColumns::new(columns, rows)];
	intern_dictionary_columns(catalog, &mut Transaction::Command(txn), &ringbuffer.columns, None, &mut cast)?;
	let mut built: Vec<_> = (0..rows).map(|_| shape.allocate_ringbuffer()).collect();
	cast[0].write(shape, &mut built)?;

	let batch = ringbuffer_batch_partitions(ringbuffer, &cast[0])?;
	let mut verified = HashMap::new();
	resolve_partitions(
		&mut Transaction::Command(txn),
		ObjectId::ringbuffer(ringbuffer.id),
		&batch.distinct,
		&mut verified,
	)?;
	let now = writer.clock.now();
	let event = EventColumn::new(
		populator_index(&ringbuffer.time, shape).map(|index| cast[0].view(index)).transpose()?,
	);
	let mut entries = Vec::with_capacity(rows);
	for (row, mut builder) in built.into_iter().enumerate() {
		stamp(&mut builder, &ringbuffer.name, &ringbuffer.time, shape, now, event.at(row))?;
		entries.push(builder.freeze_bytes());
	}

	let mut cache = HashMap::new();
	let inserted = insert_ringbuffer_chunks(
		catalog,
		&mut Transaction::Command(txn),
		ringbuffer,
		shape,
		&entries,
		&batch.rows,
		&verified,
		&mut cache,
		None,
	)?;
	save_all_partition_metadata(catalog, &mut Transaction::Command(txn), ringbuffer, &cache)?;
	Ok(inserted)
}

fn execute_series_insert<V: ValidationMode>(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	pending: &PendingSeriesInsert,
) -> Result<SeriesInsertResult> {
	let catalog = writer.catalog;
	let (namespace, series) = resolve_series(catalog, txn, pending)?;
	let mut metadata_by_partition: HashMap<Partition, SeriesPartitionMetadata> = HashMap::new();
	let shape = get_or_create_series_shape(catalog, &series, &mut Transaction::Command(txn))?;
	let sumtype = series.tag.map(|id| catalog.get_sumtype(&mut Transaction::Command(txn), id)).transpose()?;
	let (columns, tags) = coerce_columns(
		&pending.rows,
		&pending.batches,
		&series.columns,
		sumtype.as_ref(),
		&series.name,
		txn.identity,
	)?;
	let rows = pending.row_count();
	let inserted = if rows == 0 {
		0
	} else {
		enforce_write_policies(writer, txn, &namespace, &series.name, PolicyTargetType::Series, &columns)?;
		let checked = check_rows::<V>(
			&Checks {
				name: &series.name,
				columns: &series.columns,
				filled: &vec![Vec::new(); series.columns.len()],
				series: Some(&series),
				time: &series.time,
			},
			&columns,
			tags,
			rows,
		)?;
		insert_series_rows(writer, txn, &series, &shape, columns, checked, &mut metadata_by_partition)?
	};
	let now = writer.clock.now();
	for (partition, mut metadata) in metadata_by_partition {
		metadata.last_write_at = now;
		catalog.update_series_metadata_txn(&mut Transaction::Command(txn), series.id, partition, metadata)?;
	}
	Ok(SeriesInsertResult {
		namespace: pending.namespace.clone(),
		series: pending.series.clone(),
		inserted,
	})
}

#[inline]
fn resolve_series(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingSeriesInsert,
) -> Result<(Namespace, Series)> {
	let namespace = resolve_namespace(catalog, txn, &pending.namespace)?;
	let series = catalog
		.find_series_by_name(&mut Transaction::Command(txn), namespace.id(), &pending.series)?
		.ok_or_else(|| -> Error {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Series,
				namespace: pending.namespace.to_string(),
				name: pending.series.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})?;
	Ok((namespace, series))
}

fn insert_series_rows(
	writer: &Writer<'_>,
	txn: &mut CommandTransaction,
	series: &Series,
	shape: &RowShape,
	columns: Vec<(FieldRef, ArrayRef)>,
	checked: Checked,
	metadata_by_partition: &mut HashMap<Partition, SeriesPartitionMetadata>,
) -> Result<u64> {
	let catalog = writer.catalog;
	let rows = checked.keys.len();
	let key_index = key_column_index(series)?;
	let mut cast = [CastColumns::new(columns, rows)];
	intern_dictionary_columns(
		catalog,
		&mut Transaction::Command(txn),
		&series.columns,
		Some(&series.key),
		&mut cast,
	)?;
	let [cast] = cast;

	let key_column = series.key_column_data(checked.keys.clone());
	let mut built: Vec<_> = (0..rows).map(|_| shape.allocate_series()).collect();
	let mut views = Vec::with_capacity(series.columns.len());
	views.push(ColumnView::try_from(&key_column)?);
	for index in 0..series.columns.len() {
		if index != key_index {
			views.push(cast.view(index)?);
		}
	}
	shape.write_columns(&mut built, &views)?;
	let now = writer.clock.now();
	let event = EventColumn::new(populator_index(&series.time, shape).map(|index| views[index].clone()));

	let batch = series_batch_partitions(series, &cast, Some(&views[0]))?;
	resolve_partitions(
		&mut Transaction::Command(txn),
		ObjectId::Series(series.id),
		&batch.distinct,
		&mut HashMap::new(),
	)?;
	let assigned = assign_series_keys(
		catalog,
		&mut Transaction::Command(txn),
		series,
		&batch.rows,
		checked.keys.into_iter().map(Some).collect(),
		&checked.tags,
		metadata_by_partition,
		|_| Err(internal_error!("bulk insert into series {} kept a row without a key", series.name)),
	)?;
	for (row, builder) in built.iter_mut().enumerate() {
		stamp(builder, &series.name, &series.time, shape, now, event.at(row))?;
	}
	let written = write_series_rows(
		&mut Transaction::Command(txn),
		series,
		shape,
		SeriesRows {
			keys: assigned.storage,
			partitions: &batch.rows,
			rows: built,
		},
	)?;
	Ok(written.len() as u64)
}

fn parse_qualified_name(qualified_name: &str) -> (String, String) {
	if let Some((ns, name)) = qualified_name.rsplit_once("::") {
		(ns.to_string(), name.to_string())
	} else {
		("default".to_string(), qualified_name.to_string())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_qualified_name_simple() {
		assert_eq!(parse_qualified_name("table"), ("default".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_single_namespace() {
		assert_eq!(parse_qualified_name("ns::table"), ("ns".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_nested_namespace() {
		assert_eq!(parse_qualified_name("a::b::table"), ("a::b".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_deeply_nested_namespace() {
		assert_eq!(parse_qualified_name("a::b::c::table"), ("a::b::c".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_empty_string() {
		assert_eq!(parse_qualified_name(""), ("default".to_string(), "".to_string()));
	}
}
