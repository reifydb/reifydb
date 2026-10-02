// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet, hash_map::Entry},
	marker::PhantomData,
	sync::Arc,
};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_catalog::{
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder, SourceRowBuilder},
	pod::EncodedPodRow,
	shape::RowShape,
	table::EncodedTableRowBuilder,
};
use reifydb_core::{
	common::TimeSource,
	error::CoreError,
	interface::catalog::{
		column::Column,
		id::IndexId,
		key::PrimaryKey,
		namespace::Namespace,
		object::ObjectId,
		policy::{DataOp, PolicyTargetType},
		ringbuffer::RingBuffer,
		series::{Series, SeriesPartitionMetadata},
		storage::StorageId,
		sumtype::SumType,
		table::Table,
	},
	internal_error,
	key::{
		any::TaggedKey,
		catalog::IndexEntryKey,
		series::{PartitionedSeriesRowKey, SeriesRowKey},
	},
	partition::partition_of,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::{WithInterceptors, series_row::SeriesRowInterceptor},
	transaction::{Transaction, command::CommandTransaction},
};
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	value::{
		Value, column_view::ColumnView, constraint::Constraint, identity::IdentityId, partition::Partition,
		row_number::RowNumber, value_type::ValueType,
	},
};

use super::{
	BulkInsertResult, RingBufferInsertResult, SeriesInsertResult, TableInsertResult, coerce::with_row_note,
	validation::coerce_columns,
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
	partition::resolve_partition,
	policy::PolicyEvaluator,
	transaction::operation::table::TableOperations,
	vm::{
		instruction::dml::{
			coerce::key_out_of_range,
			columns::{CastColumns, intern_dictionary_columns},
			partition::{compute_partition_col_indices, save_all_partition_metadata},
			primary_key::{self, PrimaryKeyEncoder},
			ringbuffer_insert::insert_ringbuffer_chunks,
			series_insert::resolve_variant_tag,
			shape::{
				get_or_create_ringbuffer_shape, get_or_create_series_shape, get_or_create_table_shape,
			},
			time::resolve_time,
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
			run_all_pending::<V>(&writer, txn, pending_tables, pending_ringbuffers, pending_series)
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
	sumtype: Option<&'a SumType>,
	time: &'a TimeSource,
}

struct Checked {
	keys: Vec<u64>,
	tags: Vec<Option<u8>>,
}

fn check_rows<V: ValidationMode>(
	checks: &Checks<'_>,
	columns: &[(FieldRef, ArrayRef)],
	tags: Option<&(FieldRef, ArrayRef)>,
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
	if let (Some(sumtype), Some(tags)) = (checks.sumtype, tags) {
		let view = ColumnView::try_from(tags)?;
		for (row, variant_tag) in variant_tags.iter_mut().enumerate().take(limit(&failure)) {
			let value = match view.get_value(row) {
				Value::Any(value) => *value,
				value => value,
			};
			*variant_tag = match value {
				Value::None {
					..
				} => Some(0),
				value => match resolve_variant_tag(
					sumtype,
					&value,
					Fragment::internal(value.to_string()),
				) {
					Ok(tag) => Some(tag),
					Err(error) => {
						failure = Some((row, error));
						break;
					}
				},
			};
		}
	}

	if let TimeSource::Event {
		ts,
	} = checks.time
		&& let Some(index) = time_read_index(checks, ts)?
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

fn time_read_index(checks: &Checks<'_>, ts: &str) -> Result<Option<usize>> {
	let Some(index) = checks.columns.iter().position(|column| column.name == ts) else {
		return Ok(None);
	};
	let Some(series) = checks.series else {
		return Ok(Some(index));
	};
	if index == 0 {
		return key_column_index(series).map(Some);
	}
	let key_column = series.key.column();
	Ok(checks
		.columns
		.iter()
		.enumerate()
		.filter(|(_, column)| column.name != key_column)
		.nth(index - 1)
		.map(|(i, _)| i))
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
	writer: &Writer<'_>,
	row: &mut B,
	name: &str,
	columns: &[Column],
	time: &TimeSource,
	shape: &RowShape,
) -> Result<()> {
	let now = writer.clock.now();
	row.set_timestamps(now, now);
	if let Some(time) = resolve_time(name, columns, time, shape, row.as_slice(), now)? {
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
	let (mut columns, _) =
		coerce_columns(&pending.rows, &pending.batches, &table.columns, false, &table.name, txn.identity)?;
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
			sumtype: None,
			time: &table.time,
		},
		&columns,
		None,
		rows,
	)?;
	fill_auto_increment(catalog, txn, &table, &filled, &mut columns)?;

	let mut cast = [CastColumns::new(columns, rows)];
	intern_dictionary_columns(catalog, &mut Transaction::Command(txn), &table.columns, None, &mut cast)?;
	let mut built: Vec<EncodedTableRowBuilder> = (0..rows).map(|_| shape.allocate_table()).collect();
	cast[0].write(&shape, &mut built)?;
	for row in built.iter_mut() {
		stamp(writer, row, &table.name, &table.columns, &table.time, &shape)?;
	}
	write_table_rows(catalog, txn, &table, &shape, pending, built)
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

fn fill_auto_increment(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	table: &Table,
	filled: &[Vec<usize>],
	columns: &mut [(FieldRef, ArrayRef)],
) -> Result<()> {
	for ((column, rows), cast) in table.columns.iter().zip(filled).zip(columns.iter_mut()) {
		if rows.is_empty() {
			continue;
		}
		let current = ColumnView::try_from(&*cast)?;
		let mut builder = ColumnBuilder::with_capacity(column.constraint.get_type(), current.len());
		let mut next = rows.iter().peekable();
		for row in 0..current.len() {
			if next.next_if_eq(&&row).is_some() {
				builder.push_value(catalog.column_sequence_next_value(txn, table.id, column.id)?);
			} else {
				builder.push_value(current.get_value(row));
			}
		}
		*cast = builder.finish(&column.name);
	}
	Ok(())
}

#[inline]
fn empty_table_result(pending: &PendingTableInsert) -> TableInsertResult {
	TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: 0,
	}
}

#[inline]
fn write_table_rows(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	table: &Table,
	shape: &RowShape,
	pending: &PendingTableInsert,
	encoded_bytes_list: Vec<EncodedTableRowBuilder>,
) -> Result<TableInsertResult> {
	let total_rows = encoded_bytes_list.len();
	let row_numbers = catalog.next_row_number_batch(txn, table.id, total_rows as u64)?;
	let pk_def = primary_key::get_primary_key(catalog, &mut Transaction::Command(txn), table)?;

	let mut owned_rows = encoded_bytes_list;
	txn.insert_table(table, shape, &row_numbers, &mut owned_rows)?;

	if let Some(ref pk_def) = pk_def
		&& !owned_rows.is_empty()
	{
		let encoder = PrimaryKeyEncoder::new(pk_def, table)?;
		for (row, &row_number) in owned_rows.iter().zip(row_numbers.iter()) {
			write_primary_key_index(txn, table, shape, pk_def, &encoder, row, row_number)?;
		}
	}

	Ok(TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: total_rows as u64,
	})
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

fn write_primary_key_index(
	txn: &mut CommandTransaction,
	table: &Table,
	shape: &RowShape,
	pk_def: &PrimaryKey,
	encoder: &PrimaryKeyEncoder,
	row: &[u8],
	row_number: RowNumber,
) -> Result<()> {
	let index_key = encoder.encode(shape, row);
	let index_entry_key = IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key);

	if txn.contains(&index_entry_key)? {
		let key_columns = pk_def.columns.iter().map(|c| c.name.clone()).collect();
		return Err(CoreError::PrimaryKeyViolation {
			fragment: Fragment::None,
			table_name: table.name.clone(),
			key_columns,
		}
		.into());
	}

	txn.set(&index_entry_key, EncodedPodRow::new(&u64::from(row_number).to_be_bytes()).into_bytes())?;
	Ok(())
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
		false,
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
			sumtype: None,
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

	let partition_col_indices = compute_partition_col_indices(ringbuffer);
	let partition_views =
		partition_col_indices.iter().map(|&index| cast[0].view(index)).collect::<Result<Vec<_>>>()?;
	let mut entries = Vec::with_capacity(rows);
	for (row, mut builder) in built.into_iter().enumerate() {
		let partition_key: Vec<Value> = partition_views.iter().map(|view| view.get_value(row)).collect();
		let partition = if partition_col_indices.is_empty() {
			None
		} else {
			Some(partition_of(&ringbuffer.columns, &ringbuffer.partition_by, &partition_key))
		};
		stamp(writer, &mut builder, &ringbuffer.name, &ringbuffer.columns, &ringbuffer.time, shape)?;
		entries.push((builder.freeze_bytes(), partition_key, partition));
	}

	let mut cache = HashMap::new();
	let inserted = insert_ringbuffer_chunks(
		catalog,
		&mut Transaction::Command(txn),
		ringbuffer,
		shape,
		&entries,
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
		sumtype.is_some(),
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
				sumtype: sumtype.as_ref(),
				time: &series.time,
			},
			&columns,
			tags.as_ref(),
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
	{
		let mut views = Vec::with_capacity(series.columns.len());
		views.push(ColumnView::try_from(&key_column)?);
		for index in 0..series.columns.len() {
			if index != key_index {
				views.push(cast.view(index)?);
			}
		}
		shape.write_columns(&mut built, &views)?;
	}

	let partition_views = series_partition_col_indices(series)?
		.into_iter()
		.map(|index| cast.view(index))
		.collect::<Result<Vec<_>>>()?;
	let storage = StorageId::series(series.id);
	let mut verified: HashSet<Partition> = HashSet::new();
	let mut storage_keys: Vec<TaggedKey> = Vec::with_capacity(rows);
	for (row, ((builder, &key_value), &variant_tag)) in
		built.iter_mut().zip(&checked.keys).zip(&checked.tags).enumerate()
	{
		let partition_values: Vec<Value> = partition_views.iter().map(|view| view.get_value(row)).collect();
		let partition = if partition_values.is_empty() {
			Partition::default()
		} else {
			partition_of(&series.columns, &series.partition_by, &partition_values)
		};
		let metadata = match metadata_by_partition.entry(partition) {
			Entry::Occupied(entry) => entry.into_mut(),
			Entry::Vacant(entry) => {
				let loaded = catalog
					.find_series_metadata(&mut Transaction::Command(txn), series.id, partition)?
					.unwrap_or_default();
				entry.insert(loaded)
			}
		};

		metadata.sequence_counter += 1;
		let sequence = metadata.sequence_counter;
		let key: TaggedKey = if partition_values.is_empty() {
			SeriesRowKey {
				storage,
				variant_tag,
				key: key_value,
				sequence,
			}
			.into()
		} else {
			resolve_partition(
				&mut Transaction::Command(txn),
				ObjectId::Series(series.id),
				partition,
				&partition_values,
				&mut verified,
			)?;
			PartitionedSeriesRowKey::new(storage, partition, variant_tag, key_value, sequence).into()
		};
		stamp(writer, builder, &series.name, &series.columns, &series.time, shape)?;
		update_series_metadata_for_insert(metadata, key_value);
		storage_keys.push(key);
	}

	if !txn.series_row_pre_insert_interceptors().is_empty() {
		SeriesRowInterceptor::pre_insert(txn, series, &mut built)?;
	}
	let written: Vec<EncodedBytes> = built.into_iter().map(|row| row.freeze_bytes()).collect();
	for (key, row) in storage_keys.iter().zip(&written) {
		txn.set(key, row.clone())?;
	}
	if !txn.series_row_post_insert_interceptors().is_empty() {
		SeriesRowInterceptor::post_insert(txn, series, &written)?;
	}
	Ok(rows as u64)
}

#[inline]
fn series_partition_col_indices(series: &Series) -> Result<Vec<usize>> {
	series.partition_by
		.iter()
		.map(|name| {
			series.columns.iter().position(|c| &c.name == name).ok_or_else(|| {
				internal_error!("series {} partition column {} not found", series.name, name)
			})
		})
		.collect()
}

#[inline]
fn update_series_metadata_for_insert(metadata: &mut SeriesPartitionMetadata, key_value: u64) {
	if metadata.row_count == 0 {
		metadata.oldest_key = key_value;
		metadata.newest_key = key_value;
	} else {
		if key_value < metadata.oldest_key {
			metadata.oldest_key = key_value;
		}
		if key_value > metadata.newest_key {
			metadata.newest_key = key_value;
		}
	}
	metadata.dirty_from_key = metadata.dirty_from_key.min(key_value);
	metadata.dirty_to_key = metadata.dirty_to_key.max(key_value.saturating_add(1));
	metadata.row_count += 1;
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
