// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{bytes::RowBuilder, shape::RowShape};
use reifydb_core::{
	interface::{
		catalog::{column::Column, object::ObjectId, series::SeriesKey},
		evaluate::TargetColumn,
		resolved::ResolvedColumn,
	},
	internal_error,
	value::column::{
		builder::ColumnBuilder, cast::cast_column_data, factory::none_typed, write::check_digest_write,
	},
};
use reifydb_evaluate::expression::eval::loses_scale;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::Constraint,
		dictionary::DictionaryId,
		system_columns::user_columns,
		value_type::ValueType,
	},
};

use super::coerce::{InputFragments, coerce_value_to_column_type, key_out_of_range};
use crate::{
	Result,
	transaction::operation::dictionary::DictionaryOperations,
	vm::{
		services::Services,
		volcano::query::{QueryContext, eval_context_from_query},
	},
};

pub(crate) struct ColumnPipeline<'a> {
	pub(crate) columns: &'a [Column],
	pub(crate) sequences: Option<ObjectId>,
	pub(crate) series_key: Option<&'a SeriesKey>,
	pub(crate) fragments: &'a InputFragments,
	pub(crate) context: &'a QueryContext,
}

pub(crate) struct Failure {
	row: usize,
	rank: usize,
	error: Error,
}

impl Failure {
	pub(crate) fn before_columns(row: usize, error: Error) -> Self {
		Self {
			row,
			rank: 0,
			error,
		}
	}

	pub(crate) fn after_columns(pipeline: &ColumnPipeline<'_>, row: usize, error: Error) -> Self {
		Self {
			row,
			rank: pipeline.columns.len() + 1,
			error,
		}
	}

	pub(crate) fn after_key(pipeline: &ColumnPipeline<'_>, row: usize, error: Error) -> Self {
		Self {
			row,
			rank: pipeline.columns.len() + 2,
			error,
		}
	}

	pub(crate) fn earliest(failures: impl IntoIterator<Item = Option<Failure>>) -> Option<Failure> {
		failures.into_iter().flatten().min_by_key(|failure| (failure.row, failure.rank))
	}
}

pub(crate) struct CastColumns {
	columns: Vec<(FieldRef, ArrayRef)>,
	fills: Vec<(usize, Vec<usize>)>,
	rows: usize,
}

impl CastColumns {
	pub(crate) fn new(columns: Vec<(FieldRef, ArrayRef)>, rows: usize) -> Self {
		Self {
			columns,
			fills: Vec::new(),
			rows,
		}
	}

	pub(crate) fn rows(&self) -> usize {
		self.rows
	}

	pub(crate) fn view(&self, index: usize) -> Result<ColumnView<'_>> {
		ColumnView::try_from(&self.columns[index])
	}

	pub(crate) fn write<B: RowBuilder>(&self, shape: &RowShape, rows: &mut [B]) -> Result<()> {
		let views = self.columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
		shape.write_columns(rows, &views)
	}
}

pub(crate) fn input_views<'a>(batch: &'a RecordBatch, columns: &[Column]) -> Result<Vec<Option<ColumnView<'a>>>> {
	let views: Vec<ColumnView<'a>> = user_columns(batch)
		.map(|(field, array)| ColumnView::try_from((array, field.as_ref())))
		.collect::<Result<_>>()?;
	let mut by_name: HashMap<&str, usize> = HashMap::new();
	for (index, view) in views.iter().enumerate() {
		by_name.insert(view.field.name().as_str(), index);
	}
	Ok(columns.iter().map(|column| by_name.get(column.name.as_str()).map(|&index| views[index].clone())).collect())
}

impl ColumnPipeline<'_> {
	pub(crate) fn cast_target_columns(
		&self,
		inputs: &[Option<ColumnView<'_>>],
		rows: usize,
		earliest: Option<Failure>,
	) -> Result<CastColumns> {
		let mut best = earliest;
		let mut columns = Vec::with_capacity(self.columns.len());
		let mut fills = Vec::new();
		let key_rank = self.columns.len() + 1;
		for (index, (column, input)) in self.columns.iter().zip(inputs).enumerate() {
			let rank = index + 1;
			let key = self.series_key.filter(|key| key.column() == column.name);
			let filled = self.filled_rows(column, input.as_ref(), rows);
			match self.cast_column(column, input.as_ref(), rows, &filled)? {
				Some(cast) => {
					if let Some(key) = key
						&& let Some(failure) = key_failure(
							key,
							&ColumnView::try_from(&cast)?,
							limit(&best, key_rank, rows),
							key_rank,
						) {
						best = Some(failure);
					}
					columns.push(cast);
				}
				None => {
					columns.push(none_typed(
						&column.name,
						column.constraint.get_type().inner_type().clone(),
						rows,
					));
					let checks = Checks {
						column,
						input: input.as_ref(),
						filled: &filled,
						limit: limit(&best, rank, rows),
						key: key.map(|key| (key, limit(&best, key_rank, rows))),
					};
					match self.first_error(&checks) {
						Some((row, is_key, error)) => {
							best = Some(Failure {
								row,
								rank: if is_key {
									key_rank
								} else {
									rank
								},
								error,
							})
						}
						None if best.is_none() => {
							return Err(internal_error!(
								"column {} failed its column cast where every cell cast passes",
								column.name
							));
						}
						None => {}
					}
				}
			}
			if !filled.is_empty() {
				fills.push((index, filled));
			}
		}
		if let Some(failure) = best {
			return Err(failure.error);
		}
		Ok(CastColumns {
			columns,
			fills,
			rows,
		})
	}

	pub(crate) fn fill_sequences(
		&self,
		services: &Services,
		txn: &mut Transaction<'_>,
		cast: &mut CastColumns,
	) -> Result<()> {
		let Some(object) = self.sequences else {
			return Ok(());
		};
		for (index, filled) in &cast.fills {
			let column = &self.columns[*index];
			let mut builder = ColumnBuilder::with_capacity(column.constraint.get_type(), cast.rows);
			{
				let current = ColumnView::try_from(&cast.columns[*index])?;
				let mut next = filled.iter().peekable();
				for row in 0..cast.rows {
					if next.next_if_eq(&&row).is_some() {
						builder.push_value(
							services.catalog
								.column_sequence_next_value(txn, object, column.id)?,
						);
					} else {
						builder.push_value(current.get_value(row));
					}
				}
			}
			cast.columns[*index] = builder.finish(&column.name);
		}
		Ok(())
	}

	fn filled_rows(&self, column: &Column, input: Option<&ColumnView<'_>>, rows: usize) -> Vec<usize> {
		let generated = self.series_key.is_some_and(|key| key.column() == column.name);
		if !generated && (self.sequences.is_none() || !column.auto_increment) {
			return Vec::new();
		}
		match input {
			None => (0..rows).collect(),
			Some(view) => (0..rows).filter(|&row| view.none_at(row)).collect(),
		}
	}

	fn cast_column(
		&self,
		column: &Column,
		input: Option<&ColumnView<'_>>,
		rows: usize,
		filled: &[usize],
	) -> Result<Option<(FieldRef, ArrayRef)>> {
		let target = column.constraint.get_type();
		let Some(input) = input else {
			if target.is_option() || filled.len() == rows {
				return Ok(Some(none_typed(&column.name, target.inner_type().clone(), rows)));
			}
			return Ok(None);
		};
		let unfilled_none =
			|view: &ColumnView<'_>, row: usize| view.none_at(row) && filled.binary_search(&row).is_err();
		if !target.is_option() && (0..rows).any(|row| unfilled_none(input, row)) {
			return Ok(None);
		}
		if let (
			ViewData::Decimal(decimals),
			ValueType::Decimal {
				scale,
				..
			},
		) = (&input.data, target.inner_type())
			&& decimals.scale().value() > scale.value()
			&& (0..rows).any(|row| !input.none_at(row) && loses_scale(&input.get_value(row), &target))
		{
			return Ok(None);
		}

		let ident = self.fragments.column(&column.name);
		let resolved = ResolvedColumn::new(ident.clone(), self.context.source.clone().unwrap(), column.clone());
		let base = eval_context_from_query(self.context);
		let mut eval_ctx = base.with_eval_empty();
		eval_ctx.target = Some(TargetColumn::Resolved(resolved));
		if check_digest_write(input, &target, || ident.clone()).is_err() {
			return Ok(None);
		}
		let Ok(cast) = cast_column_data(&eval_ctx, input, target.clone(), || ident.clone()) else {
			return Ok(None);
		};

		let view = ColumnView::try_from(&cast)?;
		if !target.is_option() && (0..rows).any(|row| unfilled_none(&view, row)) {
			return Ok(None);
		}
		match (target.inner_type(), column.constraint.constraint()) {
			(ValueType::Utf8, Some(Constraint::MaxBytes(max))) => {
				let max: usize = (*max).into();
				if (0..rows).any(|row| view.get_str(row).is_some_and(|text| text.len() > max)) {
					return Ok(None);
				}
			}
			(ValueType::Blob, Some(Constraint::MaxBytes(max))) => {
				let max: usize = (*max).into();
				if (0..rows).any(|row| view.get_bytes(row).is_some_and(|bytes| bytes.len() > max)) {
					return Ok(None);
				}
			}
			_ => {}
		}
		Ok(Some(cast))
	}

	fn first_error(&self, checks: &Checks<'_, '_>) -> Option<(usize, bool, Error)> {
		let column = checks.column;
		let ident = self.fragments.column(&column.name);
		for row in 0..checks.limit {
			if checks.filled.binary_search(&row).is_ok() {
				continue;
			}
			let value = checks.input.map(|view| view.get_value(row)).unwrap_or_else(Value::none);
			let resolved = ResolvedColumn::new(
				ident.clone(),
				self.context.source.clone().unwrap(),
				column.clone(),
			);
			let mut value = match coerce_value_to_column_type(
				value,
				column.constraint.get_type(),
				resolved,
				self.context,
			) {
				Ok(value) => value,
				Err(error) => return Some((row, false, error)),
			};
			if let Err(mut error) = column.constraint.coerce(&mut value) {
				error.0.fragment = ident;
				return Some((row, false, error));
			}
			if let Some((key, key_limit)) = checks.key
				&& row < key_limit
				&& !matches!(value, Value::None { .. })
				&& key.key_to_u64(value.clone()).is_none()
			{
				return Some((row, true, key_out_of_range(key, &value)));
			}
		}
		None
	}
}

struct Checks<'a, 'v> {
	column: &'a Column,
	input: Option<&'a ColumnView<'v>>,
	filled: &'a [usize],
	limit: usize,
	key: Option<(&'a SeriesKey, usize)>,
}

fn limit(best: &Option<Failure>, rank: usize, rows: usize) -> usize {
	match best {
		None => rows,
		Some(failure) if rank < failure.rank => failure.row + 1,
		Some(failure) => failure.row,
	}
}

fn key_failure(key: &SeriesKey, view: &ColumnView<'_>, limit: usize, rank: usize) -> Option<Failure> {
	let keys = key.keys_to_u64(view);
	(0..limit).find(|&row| keys[row].is_none() && !view.none_at(row)).map(|row| Failure {
		row,
		rank,
		error: key_out_of_range(key, &view.get_value(row)),
	})
}

pub(crate) fn intern_dictionary_columns(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	columns: &[Column],
	series_key: Option<&SeriesKey>,
	batches: &mut [CastColumns],
) -> Result<()> {
	if batches.iter().all(|batch| batch.rows == 0) {
		return Ok(());
	}
	let mut dictionaries: Vec<(DictionaryId, Vec<usize>)> = Vec::new();
	for (index, column) in columns.iter().enumerate() {
		let Some(dictionary_id) = column.dictionary_id else {
			continue;
		};
		if series_key.is_some_and(|key| key.column() == column.name) {
			continue;
		}
		match dictionaries.iter_mut().find(|(id, _)| *id == dictionary_id) {
			Some((_, indices)) => indices.push(index),
			None => dictionaries.push((dictionary_id, vec![index])),
		}
	}

	for (dictionary_id, indices) in dictionaries {
		let dictionary = catalog.find_dictionary(txn, dictionary_id)?.ok_or_else(|| {
			internal_error!(
				"Dictionary {:?} not found for column {}",
				dictionary_id,
				columns[indices[0]].name
			)
		})?;

		let mut values = Vec::new();
		for batch in batches.iter() {
			let views = indices.iter().map(|&index| batch.view(index)).collect::<Result<Vec<_>>>()?;
			for row in 0..batch.rows {
				for view in &views {
					if !view.none_at(row) {
						values.push(view.get_value(row));
					}
				}
			}
		}

		let mut ids = txn.intern_values(&dictionary, values)?.into_iter();
		for batch in batches.iter_mut() {
			let mut builders: Vec<ColumnBuilder> = indices
				.iter()
				.map(|_| ColumnBuilder::with_capacity(ValueType::DictionaryId, batch.rows))
				.collect();
			{
				let views =
					indices.iter().map(|&index| batch.view(index)).collect::<Result<Vec<_>>>()?;
				for row in 0..batch.rows {
					for (view, builder) in views.iter().zip(builders.iter_mut()) {
						let id = if view.none_at(row) {
							dictionary.id_type.none()
						} else {
							ids.next().expect(
								"the dictionary returns one id per interned value",
							)
						};
						builder.push_value(id.to_value());
					}
				}
			}
			for (&index, builder) in indices.iter().zip(builders) {
				batch.columns[index] = builder.finish(&columns[index].name);
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{str::FromStr, sync::Arc};

	use arrow_array::RecordBatch;
	use reifydb_catalog::catalog::{
		dictionary::DictionaryToCreate,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	};
	use reifydb_codec::row::{bytes::RowBuilder, shape::RowShape};
	use reifydb_core::{
		common::TimeSource,
		interface::{
			catalog::{id::NamespaceId, table::Table},
			resolved::{ResolvedColumn, ResolvedNamespace, ResolvedObject, ResolvedTable},
		},
		value::{batch::batch, column::builder::ColumnBuilder},
	};
	use reifydb_evaluate::stack::SymbolTable;
	use reifydb_rql::{nodes::InlineDataNode, query::QueryPlan};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::{
		fragment::Fragment,
		params::Params,
		value::{
			Value,
			blob::Blob,
			constraint::{TypeConstraint, precision::Precision, scale::Scale},
			date::Date,
			datetime::DateTime,
			decimal::Decimal,
			duration::Duration,
			identity::IdentityId,
			system_columns::column_view,
			time::Time,
			uuid::Uuid4,
			value_type::ValueType,
		},
	};

	use super::{ColumnPipeline, input_views, intern_dictionary_columns};
	use crate::{
		transaction::operation::dictionary::DictionaryOperations,
		vm::{
			instruction::dml::{
				coerce::{InputFragments, coerce_value_to_column_type},
				shape::get_or_create_table_shape,
			},
			services::Services,
			volcano::query::{QueryContext, query_budget},
		},
	};

	struct Fixture {
		services: Arc<Services>,
		txn: AdminTransaction,
		table: Table,
		shape: RowShape,
		context: QueryContext,
	}

	struct Spec {
		name: &'static str,
		input: Option<ValueType>,
		target: ValueType,
		values: Vec<Value>,
		auto_increment: bool,
		dictionary: bool,
	}

	fn spec(name: &'static str, input: ValueType, target: ValueType, values: Vec<Value>) -> Spec {
		Spec {
			name,
			input: Some(input),
			target,
			values,
			auto_increment: false,
			dictionary: false,
		}
	}

	fn option(ty: ValueType) -> ValueType {
		ValueType::Option(Box::new(ty))
	}

	fn decimal(precision: u8, scale: u8) -> ValueType {
		ValueType::decimal(Precision::new(precision), Scale::new(scale))
	}

	fn dec(text: &str) -> Value {
		Value::Decimal(Decimal::from_str(text).unwrap())
	}

	fn none(ty: ValueType) -> Value {
		Value::none_of(ty)
	}

	fn specs() -> Vec<Spec> {
		let uuid = Uuid4::generate();
		let identity = IdentityId::system();
		let date = Date::from_ymd(2024, 2, 29).unwrap();
		let time = Time::from_hms(13, 14, 15).unwrap();
		let duration = Duration::from_seconds(90).unwrap();
		let at = DateTime::from_millis(1_700_000_000_000);
		vec![
			Spec {
				name: "id",
				input: Some(option(ValueType::Int4)),
				target: ValueType::Int4,
				values: vec![
					Value::Int4(1),
					none(ValueType::Int4),
					Value::Int4(3),
					none(ValueType::Int4),
				],
				auto_increment: true,
				dictionary: false,
			},
			Spec {
				name: "seq",
				input: None,
				target: ValueType::Uint8,
				values: vec![],
				auto_increment: true,
				dictionary: false,
			},
			spec(
				"b",
				ValueType::Boolean,
				ValueType::Boolean,
				vec![
					Value::Boolean(true),
					Value::Boolean(false),
					Value::Boolean(true),
					Value::Boolean(false),
				],
			),
			spec(
				"ob",
				option(ValueType::Boolean),
				option(ValueType::Boolean),
				vec![
					none(ValueType::Boolean),
					Value::Boolean(true),
					none(ValueType::Boolean),
					Value::Boolean(false),
				],
			),
			spec(
				"i1",
				ValueType::Int1,
				ValueType::Int1,
				vec![Value::Int1(-128), Value::Int1(0), Value::Int1(7), Value::Int1(127)],
			),
			spec(
				"i2",
				ValueType::Int2,
				ValueType::Int8,
				vec![Value::Int2(7), Value::Int2(-3), Value::Int2(0), Value::Int2(i16::MAX)],
			),
			spec(
				"i4",
				option(ValueType::Int4),
				option(ValueType::Int4),
				vec![none(ValueType::Int4), Value::Int4(2), none(ValueType::Int4), Value::Int4(-4)],
			),
			spec(
				"i8",
				ValueType::Int8,
				option(ValueType::Int8),
				vec![Value::Int8(i64::MIN), Value::Int8(1), Value::Int8(2), Value::Int8(i64::MAX)],
			),
			spec(
				"i16",
				ValueType::Int16,
				ValueType::Int16,
				vec![
					Value::Int16(i128::MIN),
					Value::Int16(0),
					Value::Int16(5),
					Value::Int16(i128::MAX),
				],
			),
			spec(
				"u1",
				ValueType::Uint1,
				option(ValueType::Uint1),
				vec![Value::Uint1(0), Value::Uint1(255), Value::Uint1(1), Value::Uint1(2)],
			),
			spec(
				"u2",
				ValueType::Uint2,
				ValueType::Uint4,
				vec![Value::Uint2(0), Value::Uint2(u16::MAX), Value::Uint2(1), Value::Uint2(2)],
			),
			spec(
				"u8",
				ValueType::Uint8,
				ValueType::Uint8,
				vec![Value::Uint8(0), Value::Uint8(u64::MAX), Value::Uint8(1), Value::Uint8(2)],
			),
			spec(
				"u16",
				option(ValueType::Uint16),
				option(ValueType::Uint16),
				vec![
					Value::Uint16(u128::MAX),
					none(ValueType::Uint16),
					Value::Uint16(1),
					none(ValueType::Uint16),
				],
			),
			spec(
				"f4",
				ValueType::Float4,
				ValueType::Float4,
				vec![
					Value::float4(1.5_f32),
					Value::float4(-2.5_f32),
					Value::float4(0.0_f32),
					Value::float4(3.25_f32),
				],
			),
			spec(
				"f8",
				option(ValueType::Float8),
				option(ValueType::Float8),
				vec![
					none(ValueType::Float8),
					Value::float8(0.5),
					Value::float8(-2.5),
					Value::float8(1e10),
				],
			),
			spec(
				"f84",
				ValueType::Float8,
				ValueType::Float4,
				vec![Value::float8(1.5), Value::float8(-0.25), Value::float8(2.0), Value::float8(8.0)],
			),
			spec(
				"s",
				ValueType::Utf8,
				ValueType::Utf8,
				vec![
					Value::Utf8("x".into()),
					Value::Utf8(String::new()),
					Value::Utf8("long enough text".into()),
					Value::Utf8("y".into()),
				],
			),
			Spec {
				name: "os",
				input: Some(option(ValueType::Utf8)),
				target: option(ValueType::Utf8),
				values: vec![
					Value::Utf8("x".into()),
					none(ValueType::Utf8),
					Value::Utf8("y".into()),
					Value::Utf8("x".into()),
				],
				auto_increment: false,
				dictionary: true,
			},
			Spec {
				name: "r",
				input: Some(ValueType::Utf8),
				target: ValueType::Utf8,
				values: vec![
					Value::Utf8("a".into()),
					Value::Utf8("y".into()),
					Value::Utf8("a".into()),
					Value::Utf8("z".into()),
				],
				auto_increment: false,
				dictionary: true,
			},
			spec(
				"bl",
				ValueType::Blob,
				option(ValueType::Blob),
				vec![
					Value::Blob(Blob::new(vec![0xde, 0xad])),
					Value::Blob(Blob::new(vec![])),
					Value::Blob(Blob::new(vec![1])),
					Value::Blob(Blob::new(vec![2, 3, 4])),
				],
			),
			spec("m", decimal(10, 4), decimal(12, 2), vec![dec("1.5"), dec("2.25"), dec("3"), dec("-0.5")]),
			spec(
				"om",
				option(decimal(12, 2)),
				option(decimal(12, 2)),
				vec![none(decimal(12, 2)), dec("7.25"), dec("0"), none(decimal(12, 2))],
			),
			spec("d", ValueType::Date, ValueType::Date, vec![Value::Date(date); 4]),
			spec(
				"dt",
				option(ValueType::DateTime),
				option(ValueType::DateTime),
				vec![
					Value::DateTime(at),
					none(ValueType::DateTime),
					Value::DateTime(at),
					Value::DateTime(at),
				],
			),
			spec("t", ValueType::Time, ValueType::Time, vec![Value::Time(time); 4]),
			spec(
				"du",
				ValueType::Duration,
				option(ValueType::Duration),
				vec![Value::Duration(duration); 4],
			),
			spec("u4", ValueType::Uuid4, ValueType::Uuid4, vec![Value::Uuid4(uuid); 4]),
			spec("u7", ValueType::Uuid7, option(ValueType::Uuid7), vec![Value::Uuid7(identity.value()); 4]),
			spec("id7", ValueType::IdentityId, ValueType::IdentityId, vec![Value::IdentityId(identity); 4]),
			spec(
				"txt",
				ValueType::Utf8,
				ValueType::Int4,
				vec![
					Value::Utf8("12".into()),
					Value::Utf8("-7".into()),
					Value::Utf8("0".into()),
					Value::Utf8("2147483647".into()),
				],
			),
			Spec {
				name: "gone",
				input: None,
				target: option(ValueType::Int4),
				values: vec![],
				auto_increment: false,
				dictionary: false,
			},
		]
	}

	fn input(specs: &[Spec]) -> RecordBatch {
		batch(specs
			.iter()
			.filter_map(|spec| {
				let ty = spec.input.clone()?;
				let mut builder = ColumnBuilder::with_capacity(ty, spec.values.len());
				for value in &spec.values {
					builder.push_value(value.clone());
				}
				Some(builder.finish(spec.name))
			})
			.collect())
		.unwrap()
	}

	fn fixture(specs: &[Spec]) -> Fixture {
		let services = Services::testing();
		let mut txn = create_test_admin_transaction();
		let namespace = services
			.catalog
			.create_namespace(
				&mut txn,
				NamespaceToCreate {
					namespace_fragment: None,
					name: "ns".to_string(),
					local_name: "ns".to_string(),
					parent_id: NamespaceId::ROOT,
					grpc: None,
					token: None,
				},
			)
			.unwrap();
		let dictionary = services
			.catalog
			.create_dictionary(
				&mut txn,
				DictionaryToCreate {
					name: Fragment::internal("d"),
					namespace: namespace.id(),
					value_type: ValueType::Utf8,
					id_type: ValueType::Uint2,
				},
			)
			.unwrap();
		let columns = specs
			.iter()
			.map(|spec| TableColumnToCreate {
				name: Fragment::internal(spec.name),
				fragment: Fragment::internal(spec.name),
				constraint: TypeConstraint::unconstrained(spec.target.clone()),
				properties: vec![],
				auto_increment: spec.auto_increment,
				dictionary_id: spec.dictionary.then_some(dictionary.id),
			})
			.collect();
		let table = services
			.catalog
			.create_table(
				&mut txn,
				TableToCreate {
					name: Fragment::internal("t"),
					namespace: namespace.id(),
					columns,
					primary_key_columns: None,
					partition_by: vec![],
					time: TimeSource::None,
				},
			)
			.unwrap();
		let shape = get_or_create_table_shape(&services.catalog, &table, &mut Transaction::Admin(&mut txn))
			.unwrap();
		let resolved_namespace = ResolvedNamespace::new(Fragment::internal("ns"), namespace);
		let context = QueryContext {
			services: services.clone(),
			source: Some(ResolvedObject::Table(ResolvedTable::new(
				Fragment::internal("t"),
				resolved_namespace,
				table.clone(),
			))),
			batch_size: 128,
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: IdentityId::system(),
			memory: query_budget(&services),
		};
		Fixture {
			services,
			txn,
			table,
			shape,
			context,
		}
	}

	fn per_cell_rows(fixture: &mut Fixture, input: &RecordBatch) -> Vec<Vec<u8>> {
		let mut txn = Transaction::Admin(&mut fixture.txn);
		let mut rows = Vec::new();
		for row in 0..input.num_rows() {
			let mut encoded = fixture.shape.allocate_table();
			for (index, column) in fixture.table.columns.iter().enumerate() {
				let mut value = column_view(input, &column.name)
					.unwrap()
					.map(|view| view.get_value(row))
					.unwrap_or_else(Value::none);
				if column.auto_increment && matches!(value, Value::None { .. }) {
					value = fixture
						.services
						.catalog
						.column_sequence_next_value(&mut txn, fixture.table.id, column.id)
						.unwrap();
				}
				let resolved = ResolvedColumn::new(
					Fragment::internal(&column.name),
					fixture.context.source.clone().unwrap(),
					column.clone(),
				);
				let mut value = coerce_value_to_column_type(
					value,
					column.constraint.get_type(),
					resolved,
					&fixture.context,
				)
				.unwrap();
				column.constraint.coerce(&mut value).unwrap();
				let value = match column.dictionary_id {
					Some(id) => {
						let dictionary = fixture
							.services
							.catalog
							.find_dictionary(&mut txn, id)
							.unwrap()
							.unwrap();
						match value {
							Value::None {
								..
							} => dictionary.id_type.none().to_value(),
							value => txn
								.insert_into_dictionary(&dictionary, &value)
								.unwrap()
								.to_value(),
						}
					}
					None => value,
				};
				fixture.shape.set_value(&mut encoded, index, &value);
			}
			rows.push(encoded.as_slice().to_vec());
		}
		rows
	}

	fn pipeline_rows(fixture: &mut Fixture, input: &RecordBatch) -> Vec<Vec<u8>> {
		let fragments = InputFragments::of(&QueryPlan::InlineData(InlineDataNode {
			rows: vec![],
		}));
		let pipeline = ColumnPipeline {
			columns: &fixture.table.columns,
			sequences: Some(fixture.table.id.into()),
			series_key: None,
			fragments: &fragments,
			context: &fixture.context,
		};
		let mut txn = Transaction::Admin(&mut fixture.txn);
		let inputs = input_views(input, &fixture.table.columns).unwrap();
		let mut cast = pipeline.cast_target_columns(&inputs, input.num_rows(), None).unwrap();
		pipeline.fill_sequences(&fixture.services, &mut txn, &mut cast).unwrap();
		let mut batches = [cast];
		intern_dictionary_columns(
			&fixture.services.catalog,
			&mut txn,
			pipeline.columns,
			pipeline.series_key,
			&mut batches,
		)
		.unwrap();
		let mut rows: Vec<_> = (0..input.num_rows()).map(|_| fixture.shape.allocate_table()).collect();
		batches[0].write(&fixture.shape, &mut rows).unwrap();
		rows.iter().map(|row| row.as_slice().to_vec()).collect()
	}

	#[test]
	fn insert_and_update_store_the_same_rows_for_every_type() {
		// The pipeline must store exactly the bytes the per-cell cast, intern and set_value stored.
		let specs = specs();
		let input = input(&specs);
		let expected = per_cell_rows(&mut fixture(&specs), &input);
		let actual = pipeline_rows(&mut fixture(&specs), &input);

		assert_eq!(actual.len(), expected.len());
		for (row, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
			assert_eq!(actual, expected, "row {row} differs from the per-cell write");
		}
	}
}
