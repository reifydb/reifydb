// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod table;
pub mod view;

use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::{EncodedBytes, SHAPE_HEADER_SIZE, SourceRowBuilder, read_created_at},
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	interface::{catalog::column::Column as CatalogColumn, evaluate::TargetColumn},
	value::{
		batch::empty_batch,
		column::{cast::cast_column_data, factory::none_typed, write::check_digest_write},
	},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{
		column_view::{ColumnView, ViewData},
		datetime::DateTime,
		identity::IdentityId,
		system_columns::{created_at, time, updated_at, user_columns},
		value_type::field::{from_field, to_field},
	},
};

use crate::{error::FlowSinkError, operator::with_system_columns_of, txn::FlowGet};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
static EMPTY_ROUTINES: LazyLock<Routines> = LazyLock::new(Routines::empty);

pub fn coerce_columns(
	columns: &RecordBatch,
	target_columns: &[CatalogColumn],
	runtime_context: &RuntimeContext,
) -> Result<RecordBatch> {
	let row_count = columns.num_rows();
	if row_count == 0 {
		return Ok(empty_batch());
	}

	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	let user: Vec<(&FieldRef, &ArrayRef)> = user_columns(columns).collect();
	if user.len() == target_columns.len() {
		let mut unchanged = true;
		for ((field, array), target_col) in user.iter().zip(target_columns) {
			if field.name() != target_col.name.as_str()
				|| ColumnView::try_from((*array, field.as_ref()))?.get_type()
					!= target_col.constraint.get_type()
			{
				unchanged = false;
				break;
			}
		}
		if unchanged {
			return Ok(columns.clone());
		}
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	let session = EvalContext {
		params: &EMPTY_PARAMS,
		symbols: &EMPTY_SYMBOL_TABLE,
		routines: &EMPTY_ROUTINES,
		runtime_context,
		identity: IdentityId::system(),
		batch: empty_batch(),
		row_count: 1,
		target: None,
		take: None,
	};
	let mut ctx = session.with_eval(columns.clone(), row_count);

	for target_col in target_columns {
		let target_type = target_col.constraint.get_type();

		ctx.target = Some(TargetColumn::of_column(None, target_col));

		if let Some(index) =
			columns.schema_ref().fields().iter().position(|field| field.name() == &target_col.name)
		{
			let source_col =
				ColumnView::try_from((columns.column(index), columns.schema_ref().field(index)))?;
			if source_col.get_type() == target_type {
				result_columns.push((
					columns.schema_ref().fields()[index].clone(),
					columns.column(index).clone(),
				));
				continue;
			}
			check_digest_write(&source_col, &target_type, Fragment::internal(&target_col.name))?;
			if let (
				Some(dictionary_id),
				ViewData::DictionaryId {
					..
				},
			) = (target_col.dictionary_id, &source_col.data)
			{
				let mut field_type = from_field(&source_col.field)?;
				field_type.dictionary_id = Some(dictionary_id);
				let field: FieldRef = Arc::new(to_field(&target_col.name, &field_type));
				result_columns.push((field, source_col.array().slice(0, source_col.len())));
				continue;
			}
			let casted = cast_column_data(
				&ctx,
				&source_col,
				target_type.clone(),
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(casted);
		} else {
			result_columns.push(none_typed(&target_col.name, target_type, row_count))
		}
	}

	with_system_columns_of(result_columns, columns)
}

pub fn shape_field_columns(columns: &RecordBatch, shape: &RowShape) -> Vec<usize> {
	shape.field_names()
		.map(|field_name| {
			columns.schema_ref()
				.fields()
				.iter()
				.position(|field| field.name() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in the batch", field_name))
		})
		.collect()
}

pub fn stored_created_at<T: FlowGet + ?Sized>(
	txn: &mut T,
	rows: &[(EncodedKey, Option<EncodedKey>)],
) -> Result<Vec<Option<DateTime>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}
	let mut reads = Vec::with_capacity(rows.len() * 2);
	for (post, pre) in rows {
		reads.push(post.clone());
		if let Some(pre) = pre {
			reads.push(pre.clone());
		}
	}
	let mut stored = txn.get(&reads)?.into_iter().map(|row| {
		row.filter(|row| row.len() >= SHAPE_HEADER_SIZE)
			.map(|row| read_created_at(&row))
			.filter(|c| !c.is_epoch())
	});
	Ok(rows.iter()
		.map(|(_, pre)| {
			let post = stored.next().flatten();
			let pre = pre.as_ref().and_then(|_| stored.next().flatten());
			post.or(pre)
		})
		.collect())
}

pub struct SourceRowEncoder<'a> {
	shape: &'a RowShape,
	views: Vec<ColumnView<'a>>,
	row_count: usize,
	created_at: &'a [DateTime],
	updated_at: &'a [DateTime],
	time: &'a [DateTime],
}

impl<'a> SourceRowEncoder<'a> {
	pub fn new(columns: &'a RecordBatch, shape: &'a RowShape, field_columns: &[usize]) -> Result<Self> {
		let views = field_columns
			.iter()
			.map(|&index| ColumnView::try_from((columns.column(index), columns.schema_ref().field(index))))
			.collect::<Result<Vec<_>>>()?;
		Ok(Self {
			shape,
			views,
			row_count: columns.num_rows(),
			created_at: created_at(columns)?,
			updated_at: updated_at(columns)?,
			time: time(columns)?,
		})
	}

	pub fn with_created_at(self, created_at: &'a [DateTime]) -> Self {
		Self {
			created_at,
			..self
		}
	}

	pub fn encode_all(&self) -> Result<Vec<EncodedBytes>> {
		match self.shape.family() {
			RowFamily::Table => {
				self.stamp_rows((0..self.row_count).map(|_| self.shape.allocate_table()).collect())
			}
			RowFamily::Series => {
				self.stamp_rows((0..self.row_count).map(|_| self.shape.allocate_series()).collect())
			}
			RowFamily::RingBuffer => {
				self.stamp_rows((0..self.row_count).map(|_| self.shape.allocate_ringbuffer()).collect())
			}
			RowFamily::Queue => {
				self.stamp_rows((0..self.row_count).map(|_| self.shape.allocate_queue()).collect())
			}
			other => Err(Error::from(FlowSinkError::NotASourceFamily {
				family: format!("{:?}", other),
			})),
		}
	}

	fn stamp_rows<B: SourceRowBuilder>(&self, mut rows: Vec<B>) -> Result<Vec<EncodedBytes>> {
		self.shape.write_columns(&mut rows, &self.views)?;

		rows.into_iter()
			.enumerate()
			.map(|(row_idx, mut encoded)| {
				let created_at = self.created_at.get(row_idx).copied().ok_or_else(|| {
					Error::from(FlowSinkError::MissingSystemColumn {
						column: "created_at",
						row_idx,
					})
				})?;
				let updated_at = self.updated_at.get(row_idx).copied().ok_or_else(|| {
					Error::from(FlowSinkError::MissingSystemColumn {
						column: "updated_at",
						row_idx,
					})
				})?;
				encoded.set_timestamps(created_at, updated_at);
				if let Some(time) = self.time.get(row_idx).copied() {
					encoded.set_time(time);
				}

				Ok(encoded.freeze_bytes())
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::UInt64Array;
	use reifydb_codec::row::{shape::RowShapeField, table::EncodedTableRow};
	use reifydb_core::{
		interface::catalog::{column::ColumnIndex, id::ColumnId},
		value::{batch::batch, column::builder::ColumnBuilder},
	};
	use reifydb_value::value::{
		Value,
		constraint::TypeConstraint,
		container::temporal_array::datetime_array,
		datetime::DateTime,
		system_columns::{SystemColumn, keep_system_columns, with_system_column},
		value_type::ValueType,
	};

	use super::*;

	fn single_field_shape() -> RowShape {
		RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("n".to_string(), ValueType::Int4)])
	}

	fn columns_with_stamps(created_at: i64, updated_at: i64, time: i64) -> RecordBatch {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Int4, 1);
		builder.push_value(Value::Int4(7));
		let columns = batch(vec![builder.finish("n")]).unwrap();
		let stamps = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef),
			(SystemColumn::CreatedAt, Arc::new(datetime_array([DateTime::from_nanos(created_at)]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array([DateTime::from_nanos(updated_at)]))),
			(SystemColumn::Time, Arc::new(datetime_array([DateTime::from_nanos(time)]))),
		];
		stamps.into_iter()
			.fold(columns, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	#[test]
	fn a_sink_row_carries_the_time_of_the_row_it_was_built_from() {
		// The only place a sink writes the row header, so dropping #time here lands every
		// materialised row at nanos 0 and a downstream flow inherits 1970. The three stamps are
		// seeded differently because copying the wrong one is as wrong as copying none.
		let shape = single_field_shape();
		let columns = columns_with_stamps(100, 200, 300);
		let field_columns = shape_field_columns(&columns, &shape);

		let [encoded] = <[_; 1]>::try_from(
			SourceRowEncoder::new(&columns, &shape, &field_columns).unwrap().encode_all().unwrap(),
		)
		.unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(
			encoded.time(),
			Some(DateTime::from_nanos(300)),
			"#time must come from the source row's own sidecar"
		);
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100));
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
	}

	#[test]
	fn a_sink_row_without_a_time_sidecar_is_written_without_one() {
		// A source with no time domain produces rows with no #time, so a sink must materialise them
		// rather than reject them. Substituting a stamp here would give a time-less table's rows a
		// clock they never had, and rejecting them would make the view permanently empty.
		let shape = single_field_shape();
		let columns = keep_system_columns(
			&columns_with_stamps(100, 200, 300),
			&[SystemColumn::RowNumbers, SystemColumn::CreatedAt, SystemColumn::UpdatedAt],
		)
		.unwrap();
		let field_columns = shape_field_columns(&columns, &shape);

		let [encoded] = <[_; 1]>::try_from(
			SourceRowEncoder::new(&columns, &shape, &field_columns).unwrap().encode_all().unwrap(),
		)
		.unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(encoded.time(), None, "the sink row must carry no #time");
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100), "the wall stamps are still required");
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
	}

	fn target(name: &str, ty: ValueType) -> CatalogColumn {
		CatalogColumn {
			id: ColumnId(1),
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	fn n_with_an_extra_pool_column() -> RecordBatch {
		let mut n = ColumnBuilder::with_capacity(ValueType::Int4, 2);
		n.push_value(Value::Int4(7));
		n.push_value(Value::Int4(8));
		let mut pool = ColumnBuilder::with_capacity(ValueType::Utf8, 2);
		pool.push_value(Value::Utf8("a".to_string()));
		pool.push_value(Value::Utf8("b".to_string()));
		batch(vec![pool.finish("pool"), n.finish("n")]).unwrap()
	}

	fn names(columns: &RecordBatch) -> Vec<String> {
		user_columns(columns).map(|(field, _)| field.name().clone()).collect()
	}

	#[test]
	fn a_matching_column_is_reused_not_rebuilt() {
		// The windowed views hand the sink a batch with an extra partition column, so the
		// whole-batch shortcut misses on every call. A column whose name and type already match the
		// target must still come through as the same array, otherwise every update re-casts every
		// column; the extra column must still be dropped, otherwise it leaks into the stored row.
		let columns = n_with_an_extra_pool_column();
		let coerced = coerce_columns(&columns, &[target("n", ValueType::Int4)], &RuntimeContext::testing(0, 0))
			.unwrap();

		assert_eq!(
			names(&coerced),
			vec!["n".to_string()],
			"only the target column survives, the extra is dropped"
		);
		let n = coerced.schema_ref().index_of("n").unwrap();
		let source = columns.schema_ref().index_of("n").unwrap();
		assert!(
			Arc::ptr_eq(coerced.column(n), columns.column(source)),
			"a column already of the target type must be reused, not rebuilt"
		);
		assert_eq!(coerced.num_rows(), 2);
	}

	#[test]
	fn a_column_of_another_type_is_still_cast_to_the_target() {
		// Reuse must only fire on an exact type match; a narrower source reused as-is would store
		// Int4 bytes under an Int8 column and every reader would decode garbage.
		let columns = n_with_an_extra_pool_column();
		let coerced = coerce_columns(&columns, &[target("n", ValueType::Int8)], &RuntimeContext::testing(0, 0))
			.unwrap();

		let n = coerced.schema_ref().index_of("n").unwrap();
		let view = ColumnView::try_from((coerced.column(n), coerced.schema_ref().field(n))).unwrap();
		assert_eq!(view.get_type(), ValueType::Int8, "the source must be cast to the target type");
		assert_eq!(view.get_value(0), Value::Int8(7));
		assert_eq!(view.get_value(1), Value::Int8(8));
	}
}
