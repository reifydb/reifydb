// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Display, Formatter},
	slice,
	sync::{Arc, LazyLock},
};

use arrow_array::{
	Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions, TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{ArrowError, Field, FieldRef, Metadata, Schema};

use crate::{
	Result,
	error::Error,
	value::{
		column_view::ColumnView,
		container::{temporal_array::datetimes, wide_int_array::wides},
		datetime::DateTime,
		partition::Partition,
		row_number::RowNumber,
		value_type::{
			ValueType,
			field::{FieldType, field_error, to_field},
		},
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemColumn {
	RowNumbers,
	Partitions,
	CreatedAt,
	UpdatedAt,
	Time,
	CommitVersion,
}

impl SystemColumn {
	pub const ALL: [SystemColumn; 6] = [
		SystemColumn::RowNumbers,
		SystemColumn::Partitions,
		SystemColumn::CreatedAt,
		SystemColumn::UpdatedAt,
		SystemColumn::Time,
		SystemColumn::CommitVersion,
	];

	pub const fn name(self) -> &'static str {
		match self {
			SystemColumn::RowNumbers => "#rownum",
			SystemColumn::Partitions => "#partition",
			SystemColumn::CreatedAt => "#created_at",
			SystemColumn::UpdatedAt => "#updated_at",
			SystemColumn::Time => "#time",
			SystemColumn::CommitVersion => "#commit_version",
		}
	}

	pub const fn ty(self) -> ValueType {
		match self {
			SystemColumn::RowNumbers => ValueType::Uint8,
			SystemColumn::Partitions => ValueType::Uint16,
			SystemColumn::CreatedAt => ValueType::DateTime,
			SystemColumn::UpdatedAt => ValueType::DateTime,
			SystemColumn::Time => ValueType::DateTime,
			SystemColumn::CommitVersion => ValueType::Uint8,
		}
	}

	pub fn from_name(name: &str) -> Option<SystemColumn> {
		SystemColumn::ALL.into_iter().find(|column| column.name() == name)
	}

	fn rank(self) -> usize {
		SystemColumn::ALL.iter().position(|column| *column == self).expect("every system column is in ALL")
	}
}

static SYSTEM_FIELDS: LazyLock<[[FieldRef; 2]; 6]> = LazyLock::new(|| {
	SystemColumn::ALL.map(|column| {
		[false, true].map(|holds_nones| {
			let value_type = match holds_nones {
				true => ValueType::Option(Box::new(column.ty())),
				false => column.ty(),
			};
			Arc::new(to_field(
				column.name(),
				&FieldType {
					value_type: Some(value_type),
					..FieldType::default()
				},
			))
		})
	})
});

pub fn system_field(column: SystemColumn, holds_nones: bool) -> FieldRef {
	SYSTEM_FIELDS[column.rank()][usize::from(holds_nones)].clone()
}

impl Display for SystemColumn {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(self.name())
	}
}

pub fn system_column(batch: &RecordBatch, column: SystemColumn) -> Option<&ArrayRef> {
	position(batch, column.name()).map(|index| batch.column(index))
}

pub fn row_numbers(batch: &RecordBatch) -> Result<&[RowNumber]> {
	let Some(array) = present::<UInt64Array>(batch, SystemColumn::RowNumbers)? else {
		return Ok(&[]);
	};
	let values: &[u64] = array.values();
	// SAFETY: RowNumber is repr(transparent) over u64 with no niche, so the cast keeps the bounds and lifetime.
	Ok(unsafe { slice::from_raw_parts(values.as_ptr().cast::<RowNumber>(), values.len()) })
}

pub fn partitions(batch: &RecordBatch) -> Result<Vec<Partition>> {
	let Some(array) = present::<FixedSizeBinaryArray>(batch, SystemColumn::Partitions)? else {
		return Ok(Vec::new());
	};
	Ok(wides::<u128>(array).into_iter().map(Partition).collect())
}

pub fn created_at(batch: &RecordBatch) -> Result<&[DateTime]> {
	datetime_column(batch, SystemColumn::CreatedAt)
}

pub fn updated_at(batch: &RecordBatch) -> Result<&[DateTime]> {
	datetime_column(batch, SystemColumn::UpdatedAt)
}

pub fn time(batch: &RecordBatch) -> Result<&[DateTime]> {
	datetime_column(batch, SystemColumn::Time)
}

pub fn require_row_numbers(batch: &RecordBatch) -> Result<&[RowNumber]> {
	require(batch, SystemColumn::RowNumbers)?;
	row_numbers(batch)
}

pub fn require_created_at(batch: &RecordBatch) -> Result<&[DateTime]> {
	require(batch, SystemColumn::CreatedAt)?;
	created_at(batch)
}

pub fn require_updated_at(batch: &RecordBatch) -> Result<&[DateTime]> {
	require(batch, SystemColumn::UpdatedAt)?;
	updated_at(batch)
}

pub fn require_time(batch: &RecordBatch) -> Result<&[DateTime]> {
	require(batch, SystemColumn::Time)?;
	time(batch)
}

pub fn commit_versions(batch: &RecordBatch) -> Result<&[u64]> {
	match present::<UInt64Array>(batch, SystemColumn::CommitVersion)? {
		Some(array) => Ok(array.values()),
		None => Ok(&[]),
	}
}

pub fn with_system_column(batch: RecordBatch, column: SystemColumn, array: ArrayRef) -> Result<RecordBatch> {
	stamp_system_columns(batch, vec![(column, array)])
}

pub fn stamp_system_columns(batch: RecordBatch, stamps: Vec<(SystemColumn, ArrayRef)>) -> Result<RecordBatch> {
	let schema = batch.schema();
	let fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
	stamped(schema.metadata().clone(), fields, batch.columns().to_vec(), batch.num_rows(), stamps)
}

pub fn restamp_row_numbers(batch: &RecordBatch, keep: &[SystemColumn], row_numbers: ArrayRef) -> Result<RecordBatch> {
	let schema = batch.schema_ref();
	let (fields, columns): (Vec<FieldRef>, Vec<ArrayRef>) = schema
		.fields()
		.iter()
		.zip(batch.columns())
		.filter(|(field, _)| SystemColumn::from_name(field.name()).is_none_or(|column| keep.contains(&column)))
		.map(|(field, array)| (field.clone(), array.clone()))
		.unzip();
	stamped(
		schema.metadata().clone(),
		fields,
		columns,
		batch.num_rows(),
		vec![(SystemColumn::RowNumbers, row_numbers)],
	)
}

fn stamped(
	metadata: Metadata,
	mut fields: Vec<FieldRef>,
	mut columns: Vec<ArrayRef>,
	num_rows: usize,
	stamps: Vec<(SystemColumn, ArrayRef)>,
) -> Result<RecordBatch> {
	let mut row_count = num_rows;
	for (column, array) in stamps {
		let field = system_field(column, array.logical_null_count() > 0);
		if fields.is_empty() && row_count == 0 {
			row_count = array.len();
		}
		match fields.iter().position(|other| other.name() == column.name()) {
			Some(index) => {
				fields[index] = field;
				columns[index] = array;
			}
			None => {
				let index = fields
					.iter()
					.position(|other| {
						SystemColumn::from_name(other.name())
							.is_some_and(|other| other.rank() > column.rank())
					})
					.unwrap_or(fields.len());
				fields.insert(index, field);
				columns.insert(index, array);
			}
		}
	}
	RecordBatch::try_new_with_options(
		Arc::new(Schema::new_with_metadata(fields, metadata)),
		columns,
		&RecordBatchOptions::new().with_row_count(Some(row_count)),
	)
	.map_err(arrow_error)
}

pub fn keep_system_columns(batch: &RecordBatch, keep: &[SystemColumn]) -> Result<RecordBatch> {
	let indices: Vec<usize> = batch
		.schema_ref()
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| SystemColumn::from_name(field.name()).is_none_or(|column| keep.contains(&column)))
		.map(|(index, _)| index)
		.collect();
	batch.project(&indices).map_err(arrow_error)
}

pub fn is_system_field(field: &Field) -> bool {
	field.name().starts_with('#')
}

pub fn user_columns(batch: &RecordBatch) -> impl Iterator<Item = (&FieldRef, &ArrayRef)> {
	batch.schema_ref().fields().iter().zip(batch.columns()).filter(|(field, _)| !is_system_field(field))
}

pub fn column_view<'a>(batch: &'a RecordBatch, name: &str) -> Result<Option<ColumnView<'a>>> {
	match position(batch, name) {
		Some(index) => ColumnView::try_from((batch.column(index), batch.schema_ref().field(index))).map(Some),
		None => Ok(None),
	}
}

pub fn resolve_column(batch: &RecordBatch, name: &str) -> Option<usize> {
	let bare = name.strip_prefix('#').unwrap_or(name);
	SystemColumn::ALL
		.into_iter()
		.find(|column| &column.name()[1..] == bare)
		.and_then(|column| position(batch, column.name()))
		.or_else(|| position(batch, name))
}

pub fn check_user_columns(batch: &RecordBatch) -> Result<()> {
	if batch.num_rows() > 0 && batch.num_columns() == 0 {
		return Err(field_error(format!("a batch of {} rows has no columns", batch.num_rows())));
	}
	Ok(())
}

fn position(batch: &RecordBatch, name: &str) -> Option<usize> {
	batch.schema_ref().fields().iter().position(|field| field.name() == name)
}

fn present<T: 'static>(batch: &RecordBatch, column: SystemColumn) -> Result<Option<&T>> {
	let Some(array) = system_column(batch, column) else {
		return Ok(None);
	};
	if array.logical_null_count() > 0 {
		return Err(field_error(format!(
			"system column {} holds {} none rows",
			column.name(),
			array.logical_null_count()
		)));
	}
	array.as_any().downcast_ref::<T>().map(Some).ok_or_else(|| {
		field_error(format!("system column {} holds arrow type {}", column.name(), array.data_type()))
	})
}

fn require(batch: &RecordBatch, column: SystemColumn) -> Result<()> {
	match system_column(batch, column) {
		Some(_) => Ok(()),
		None => Err(field_error(format!("system column {} is missing", column.name()))),
	}
}

fn datetime_column(batch: &RecordBatch, column: SystemColumn) -> Result<&[DateTime]> {
	match present::<TimestampNanosecondArray>(batch, column)? {
		Some(array) => Ok(datetimes(array)),
		None => Ok(&[]),
	}
}

fn arrow_error(error: ArrowError) -> Error {
	field_error(error.to_string())
}

#[cfg(test)]
mod tests {
	use arrow_array::{Int32Array, UInt64Array};
	use arrow_schema::DataType;

	use super::*;
	use crate::value::{
		Value,
		container::{temporal_array::datetime_array, wide_int_array::wide_array},
	};

	fn user(name: &str, values: Vec<i32>) -> (FieldRef, ArrayRef) {
		let field = to_field(
			name,
			&FieldType {
				value_type: Some(ValueType::Int4),
				..FieldType::default()
			},
		);
		(Arc::new(field), Arc::new(Int32Array::from(values)))
	}

	fn batch(columns: Vec<(FieldRef, ArrayRef)>) -> RecordBatch {
		let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
		RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
	}

	fn names(batch: &RecordBatch) -> Vec<String> {
		batch.schema_ref().fields().iter().map(|field| field.name().clone()).collect()
	}

	fn stamps(values: Vec<i64>) -> ArrayRef {
		Arc::new(datetime_array(values.into_iter().map(DateTime::from_nanos)))
	}

	#[test]
	fn row_numbers_read_the_rownum_column_and_are_empty_when_it_is_absent() {
		// A getter that fails on an absent column would break every batch that never carried row numbers.
		let plain = batch(vec![user("a", vec![1, 2])]);
		assert!(row_numbers(&plain).unwrap().is_empty());
		let numbered =
			with_system_column(plain, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![7u64, 9])))
				.unwrap();
		assert_eq!(row_numbers(&numbered).unwrap(), &[RowNumber(7), RowNumber(9)]);
	}

	#[test]
	fn partitions_decode_the_sixteen_byte_column() {
		// Reading #partition as a u16 column would truncate every partition id above 65535.
		let wide: ArrayRef = Arc::new(wide_array([1u128 << 100, 3]));
		let with =
			with_system_column(batch(vec![user("a", vec![1, 2])]), SystemColumn::Partitions, wide).unwrap();
		assert_eq!(partitions(&with).unwrap(), vec![Partition(1u128 << 100), Partition(3)]);
	}

	#[test]
	fn datetime_getters_read_their_own_column() {
		// Mixing up #created_at, #updated_at and #time would stamp rows with the wrong instant.
		let base = batch(vec![user("a", vec![1])]);
		let base = with_system_column(base, SystemColumn::CreatedAt, stamps(vec![10])).unwrap();
		let base = with_system_column(base, SystemColumn::UpdatedAt, stamps(vec![20])).unwrap();
		let base = with_system_column(base, SystemColumn::Time, stamps(vec![30])).unwrap();
		let base =
			with_system_column(base, SystemColumn::CommitVersion, Arc::new(UInt64Array::from(vec![5u64])))
				.unwrap();
		assert_eq!(created_at(&base).unwrap(), &[DateTime::from_nanos(10)]);
		assert_eq!(updated_at(&base).unwrap(), &[DateTime::from_nanos(20)]);
		assert_eq!(time(&base).unwrap(), &[DateTime::from_nanos(30)]);
		assert_eq!(commit_versions(&base).unwrap(), &[5]);
	}

	#[test]
	fn a_getter_rejects_a_none_row() {
		// A slice getter has no way to say none, so a none row read as a value would invent a timestamp.
		let mixed: ArrayRef =
			Arc::new(TimestampNanosecondArray::from(vec![Some(1), None]).with_timezone("+00:00"));
		let with = with_system_column(batch(vec![user("a", vec![1, 2])]), SystemColumn::Time, mixed).unwrap();
		assert!(with.schema_ref().field_with_name("#time").unwrap().is_nullable());
		assert!(time(&with).is_err());
	}

	#[test]
	fn a_getter_rejects_a_column_of_the_wrong_arrow_type() {
		// Reinterpreting an int32 column as row numbers would read garbage rows.
		let (_, wrong) = user("#rownum", vec![1]);
		let field = Arc::new(Field::new("#rownum", wrong.data_type().clone(), false));
		let bad = batch(vec![(field, wrong)]);
		assert!(row_numbers(&bad).is_err());
	}

	#[test]
	fn system_columns_are_inserted_after_user_columns_in_all_order() {
		// Index based readers break if a system column lands between user columns or out of order.
		let base = batch(vec![user("a", vec![1]), user("b", vec![2])]);
		let base = with_system_column(base, SystemColumn::Time, stamps(vec![3])).unwrap();
		let base = with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])))
			.unwrap();
		let base = with_system_column(base, SystemColumn::CreatedAt, stamps(vec![4])).unwrap();
		assert_eq!(names(&base), ["a", "b", "#rownum", "#created_at", "#time"]);
	}

	#[test]
	fn a_present_system_column_is_replaced_in_place() {
		// Appending a second #rownum would leave two columns under one name.
		let base = with_system_column(
			batch(vec![user("a", vec![1])]),
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![1u64])),
		)
		.unwrap();
		let replaced =
			with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![8u64])))
				.unwrap();
		assert_eq!(names(&replaced), ["a", "#rownum"]);
		assert_eq!(row_numbers(&replaced).unwrap(), &[RowNumber(8)]);
	}

	#[test]
	fn a_system_column_on_an_empty_batch_sets_the_row_count() {
		// A named-only #rownum result must keep its rows, not collapse to the empty batch's zero rows.
		let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
		let with = with_system_column(
			empty,
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
		)
		.unwrap();
		assert_eq!(with.num_rows(), 3);
	}

	#[test]
	fn a_system_column_of_the_wrong_length_is_rejected() {
		// A short #rownum would pair rows with the wrong row numbers.
		let result = with_system_column(
			batch(vec![user("a", vec![1, 2])]),
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![1u64])),
		);
		assert!(result.is_err());
	}

	#[test]
	fn keep_drops_only_unnamed_system_columns() {
		// Dropping #rownum or a user column named with a hash, like #op, would lose data the caller needs.
		let op = Arc::new(Field::new("#op", DataType::Int32, false));
		let base = batch(vec![user("a", vec![1]), (op, Arc::new(Int32Array::from(vec![1])))]);
		let base = with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])))
			.unwrap();
		let base = with_system_column(base, SystemColumn::CreatedAt, stamps(vec![1])).unwrap();
		let base = with_system_column(base, SystemColumn::Time, stamps(vec![1])).unwrap();
		let kept = keep_system_columns(&base, &[SystemColumn::RowNumbers, SystemColumn::Time]).unwrap();
		assert_eq!(names(&kept), ["a", "#op", "#rownum", "#time"]);
		assert_eq!(kept.num_rows(), 1);
	}

	#[test]
	fn a_bare_name_resolves_to_the_system_column_first() {
		// A user column named rownum must not shadow #rownum for a bare lookup, as today.
		let base = batch(vec![user("rownum", vec![5]), user("x", vec![6])]);
		let base = with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])))
			.unwrap();
		assert_eq!(resolve_column(&base, "rownum"), Some(2));
		assert_eq!(resolve_column(&base, "#rownum"), Some(2));
		assert_eq!(resolve_column(&base, "x"), Some(1));
		assert_eq!(resolve_column(&base, "created_at"), None);
		assert_eq!(resolve_column(&base, "missing"), None);
	}

	#[test]
	fn rows_without_any_column_are_rejected() {
		// A row count with no column to carry it is the hazard; a named-only #rownum result is valid.
		let counted = RecordBatch::try_new_with_options(
			Arc::new(Schema::empty()),
			vec![],
			&RecordBatchOptions::new().with_row_count(Some(2)),
		)
		.unwrap();
		assert!(check_user_columns(&counted).is_err());
		let named = with_system_column(
			RecordBatch::new_empty(Arc::new(Schema::empty())),
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![1u64, 2])),
		)
		.unwrap();
		assert!(check_user_columns(&named).is_ok());
		assert!(check_user_columns(&RecordBatch::new_empty(Arc::new(Schema::empty()))).is_ok());
	}

	#[test]
	fn a_hash_prefix_marks_a_system_field() {
		// Counting #op as a user column would give subscriptions a phantom column.
		assert!(is_system_field(&Field::new("#rownum", DataType::UInt64, false)));
		assert!(is_system_field(&Field::new("#op", DataType::Int32, false)));
		assert!(!is_system_field(&Field::new("rownum", DataType::UInt64, false)));
	}

	#[test]
	fn column_view_finds_a_column_by_its_exact_name() {
		// A lookup that strips the hash would read the user column rownum for #rownum.
		let base = batch(vec![user("rownum", vec![5])]);
		let base = with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![9u64])))
			.unwrap();
		let user_view = column_view(&base, "rownum").unwrap().unwrap();
		assert_eq!(user_view.get_value(0), Value::Int4(5));
		let system_view = column_view(&base, "#rownum").unwrap().unwrap();
		assert_eq!(system_view.get_value(0), Value::Uint8(9));
		assert!(column_view(&base, "missing").unwrap().is_none());
	}

	#[test]
	fn require_getters_fail_naming_the_absent_column() {
		// An absent column read as empty would pair a non-empty batch with no row numbers or stamps.
		let plain = batch(vec![user("a", vec![1])]);
		for (result, name) in [
			(require_row_numbers(&plain).map(|_| ()), "#rownum"),
			(require_created_at(&plain).map(|_| ()), "#created_at"),
			(require_updated_at(&plain).map(|_| ()), "#updated_at"),
			(require_time(&plain).map(|_| ()), "#time"),
		] {
			let message = format!("{:?}", result.unwrap_err());
			assert!(message.contains(name), "{name} missing from {message}");
		}
	}

	#[test]
	fn require_getters_read_a_present_column() {
		// A require getter that read the wrong column would stamp rows with another column's values.
		let base = batch(vec![user("a", vec![1])]);
		let base = with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![4u64])))
			.unwrap();
		let base = with_system_column(base, SystemColumn::CreatedAt, stamps(vec![10])).unwrap();
		let base = with_system_column(base, SystemColumn::UpdatedAt, stamps(vec![20])).unwrap();
		let base = with_system_column(base, SystemColumn::Time, stamps(vec![30])).unwrap();
		assert_eq!(require_row_numbers(&base).unwrap(), &[RowNumber(4)]);
		assert_eq!(require_created_at(&base).unwrap(), &[DateTime::from_nanos(10)]);
		assert_eq!(require_updated_at(&base).unwrap(), &[DateTime::from_nanos(20)]);
		assert_eq!(require_time(&base).unwrap(), &[DateTime::from_nanos(30)]);
	}

	#[test]
	fn require_getters_still_reject_a_none_row() {
		// Presence alone must not let a none row through as an invented timestamp.
		let mixed: ArrayRef =
			Arc::new(TimestampNanosecondArray::from(vec![Some(1), None]).with_timezone("+00:00"));
		let with = with_system_column(batch(vec![user("a", vec![1, 2])]), SystemColumn::Time, mixed).unwrap();
		assert!(require_time(&with).is_err());
	}

	#[test]
	fn user_columns_skip_every_hash_column_in_order() {
		// Counting #rownum or #op as a user column would give callers a phantom column.
		let op = Arc::new(Field::new("#op", DataType::Int32, false));
		let base =
			batch(vec![user("a", vec![1]), (op, Arc::new(Int32Array::from(vec![2]))), user("b", vec![3])]);
		let base = with_system_column(base, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])))
			.unwrap();
		let names: Vec<&str> = user_columns(&base).map(|(field, _)| field.name().as_str()).collect();
		assert_eq!(names, ["a", "b"]);
		let (_, b) = user_columns(&base).nth(1).unwrap();
		assert_eq!(b.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 3);
		assert_eq!(user_columns(&RecordBatch::new_empty(Arc::new(Schema::empty()))).count(), 0);
	}
}
