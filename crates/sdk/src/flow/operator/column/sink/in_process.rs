// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_buffer::BooleanBuffer;
use reifydb_codec::tag::ValueKind;
use reifydb_core::value::{
	batch::batch,
	column::{builder::ColumnBuilder, push::Push},
};
use reifydb_value::{
	util::kernel::shared_predicate,
	value::{
		blob::Blob,
		column_view::ColumnView,
		constraint::{precision::Precision, scale::Scale},
		container::temporal_array::datetime_array,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{SystemColumn, stamp_system_columns},
		time::Time,
	},
};

use crate::{
	common::{extern_wasm::marshal::util::column_data_to_type_code, family::default_family_params},
	error::SdkError,
	flow::operator::{
		column::sink::{
			ColumnWriter, NoneOnNan, RowSink, column_mismatch, column_type, foreign_source,
			pass_after_push, pass_kind, pass_source, passed_column,
		},
		context::check_emit_count,
		view::{ColumnsView, PassSource},
	},
};

pub struct InProcessRowSink {
	columns: &'static [(&'static str, ValueKind)],
	cols: Vec<ColumnBuilder>,
	passed: Vec<bool>,
}

impl InProcessRowSink {
	pub fn new(columns: &'static [(&'static str, ValueKind)], row_capacity: usize) -> Result<Self, SdkError> {
		let cols = columns
			.iter()
			.map(|(_, kind)| Ok(ColumnBuilder::with_capacity(column_type(*kind)?, row_capacity)))
			.collect::<Result<_, SdkError>>()?;
		Ok(Self {
			columns,
			cols,
			passed: vec![false; columns.len()],
		})
	}

	pub fn finish(self, row_numbers: Vec<RowNumber>, now: DateTime) -> Result<RecordBatch, SdkError> {
		check_emit_count(self.cols.first().map_or(0, ColumnBuilder::len), row_numbers.len())?;
		let out = self.columns.iter().zip(self.cols).map(|((name, _), data)| data.finish(name)).collect();
		let out = batch(out)?;
		let row_count = out.num_rows();
		let mut stamps: Vec<(SystemColumn, ArrayRef)> = Vec::new();
		if !row_numbers.is_empty() {
			let row_numbers = UInt64Array::from(row_numbers.into_iter().map(|rn| rn.0).collect::<Vec<_>>());
			stamps.push((SystemColumn::RowNumbers, Arc::new(row_numbers)));
		}
		for column in [SystemColumn::CreatedAt, SystemColumn::UpdatedAt, SystemColumn::Time] {
			stamps.push((column, Arc::new(datetime_array(vec![now; row_count]))));
		}
		Ok(stamp_system_columns(out, stamps)?)
	}

	#[inline]
	fn column(&mut self, col: usize, kind: ValueKind, cell: &str) -> Result<&mut ColumnBuilder, SdkError> {
		if self.passed[col] {
			return Err(passed_column(col));
		}
		let declared = self.columns[col].1;
		if declared != kind {
			return Err(column_mismatch(col, declared, cell));
		}
		Ok(&mut self.cols[col])
	}

	#[inline]
	fn scalar<T>(&mut self, col: usize, kind: ValueKind, cell: &str) -> Result<Scalar<'_, T>, SdkError> {
		Ok(Scalar {
			builder: self.column(col, kind, cell)?,
			_t: PhantomData,
		})
	}
}

struct Scalar<'s, T> {
	builder: &'s mut ColumnBuilder,
	_t: PhantomData<fn(T)>,
}

impl<T: Copy> ColumnWriter<T> for Scalar<'_, T>
where
	ColumnBuilder: Push<T>,
{
	#[inline]
	fn push(&mut self, value: &T) -> Result<(), SdkError> {
		<ColumnBuilder as Push<T>>::push(self.builder, *value);
		Ok(())
	}

	#[inline]
	fn push_none(&mut self) -> Result<(), SdkError> {
		self.builder.push_none();
		Ok(())
	}
}

struct Text<'s>(&'s mut ColumnBuilder);

impl ColumnWriter<str> for Text<'_> {
	#[inline]
	fn push(&mut self, value: &str) -> Result<(), SdkError> {
		self.0.push(value);
		Ok(())
	}

	#[inline]
	fn push_none(&mut self) -> Result<(), SdkError> {
		self.0.push_none();
		Ok(())
	}
}

struct Bytes<'s>(&'s mut ColumnBuilder);

impl ColumnWriter<[u8]> for Bytes<'_> {
	#[inline]
	fn push(&mut self, value: &[u8]) -> Result<(), SdkError> {
		self.0.push(Blob::new(value.to_vec()));
		Ok(())
	}

	#[inline]
	fn push_none(&mut self) -> Result<(), SdkError> {
		self.0.push_none();
		Ok(())
	}
}

struct Fitted<'s> {
	builder: &'s mut ColumnBuilder,
	col: usize,
	precision: Precision,
	scale: Scale,
}

impl ColumnWriter<Decimal> for Fitted<'_> {
	#[inline]
	fn push(&mut self, value: &Decimal) -> Result<(), SdkError> {
		let fitted = value.fits(self.precision.value(), self.scale.value()).ok_or_else(|| {
			SdkError::InvalidInput(format!(
				"{value} does not fit native sink column {} of precision {} and scale {}",
				self.col,
				self.precision.value(),
				self.scale.value()
			))
		})?;
		self.builder.push(fitted);
		Ok(())
	}

	#[inline]
	fn push_none(&mut self) -> Result<(), SdkError> {
		self.builder.push_none();
		Ok(())
	}
}

impl RowSink for InProcessRowSink {
	fn u8_column(&mut self, col: usize) -> Result<impl ColumnWriter<u8> + '_, SdkError> {
		self.scalar(col, ValueKind::Uint1, "u8")
	}
	fn u16_column(&mut self, col: usize) -> Result<impl ColumnWriter<u16> + '_, SdkError> {
		self.scalar(col, ValueKind::Uint2, "u16")
	}
	fn u32_column(&mut self, col: usize) -> Result<impl ColumnWriter<u32> + '_, SdkError> {
		self.scalar(col, ValueKind::Uint4, "u32")
	}
	fn u64_column(&mut self, col: usize) -> Result<impl ColumnWriter<u64> + '_, SdkError> {
		self.scalar(col, ValueKind::Uint8, "u64")
	}
	fn u128_column(&mut self, col: usize) -> Result<impl ColumnWriter<u128> + '_, SdkError> {
		self.scalar(col, ValueKind::Uint16, "u128")
	}
	fn i8_column(&mut self, col: usize) -> Result<impl ColumnWriter<i8> + '_, SdkError> {
		self.scalar(col, ValueKind::Int1, "i8")
	}
	fn i16_column(&mut self, col: usize) -> Result<impl ColumnWriter<i16> + '_, SdkError> {
		self.scalar(col, ValueKind::Int2, "i16")
	}
	fn i32_column(&mut self, col: usize) -> Result<impl ColumnWriter<i32> + '_, SdkError> {
		self.scalar(col, ValueKind::Int4, "i32")
	}
	fn i64_column(&mut self, col: usize) -> Result<impl ColumnWriter<i64> + '_, SdkError> {
		self.scalar(col, ValueKind::Int8, "i64")
	}
	fn i128_column(&mut self, col: usize) -> Result<impl ColumnWriter<i128> + '_, SdkError> {
		self.scalar(col, ValueKind::Int16, "i128")
	}
	fn f32_column(&mut self, col: usize) -> Result<impl ColumnWriter<f32> + '_, SdkError> {
		Ok(NoneOnNan(self.scalar::<f32>(col, ValueKind::Float4, "f32")?))
	}
	fn f64_column(&mut self, col: usize) -> Result<impl ColumnWriter<f64> + '_, SdkError> {
		Ok(NoneOnNan(self.scalar::<f64>(col, ValueKind::Float8, "f64")?))
	}
	fn date_column(&mut self, col: usize) -> Result<impl ColumnWriter<Date> + '_, SdkError> {
		self.scalar(col, ValueKind::Date, "date")
	}
	fn datetime_column(&mut self, col: usize) -> Result<impl ColumnWriter<DateTime> + '_, SdkError> {
		self.scalar(col, ValueKind::DateTime, "datetime")
	}
	fn time_column(&mut self, col: usize) -> Result<impl ColumnWriter<Time> + '_, SdkError> {
		self.scalar(col, ValueKind::Time, "time")
	}
	fn duration_column(&mut self, col: usize) -> Result<impl ColumnWriter<Duration> + '_, SdkError> {
		self.scalar(col, ValueKind::Duration, "duration")
	}
	fn bool_column(&mut self, col: usize) -> Result<impl ColumnWriter<bool> + '_, SdkError> {
		self.scalar(col, ValueKind::Boolean, "bool")
	}
	fn utf8_column(&mut self, col: usize) -> Result<impl ColumnWriter<str> + '_, SdkError> {
		Ok(Text(self.column(col, ValueKind::Utf8, "utf8")?))
	}
	fn blob_column(&mut self, col: usize) -> Result<impl ColumnWriter<[u8]> + '_, SdkError> {
		Ok(Bytes(self.column(col, ValueKind::Blob, "blob")?))
	}
	fn decimal_column(&mut self, col: usize) -> Result<impl ColumnWriter<Decimal> + '_, SdkError> {
		let (precision, scale) = default_family_params(ValueKind::Decimal)
			.ok_or_else(|| SdkError::Other("decimal has no default precision and scale".to_string()))?;
		Ok(Fitted {
			builder: self.column(col, ValueKind::Decimal, "decimal")?,
			col,
			precision,
			scale,
		})
	}
	fn dictionary_id_column(&mut self, col: usize) -> Result<impl ColumnWriter<DictionaryEntryId> + '_, SdkError> {
		self.scalar(col, ValueKind::DictionaryId, "dictionary id")
	}
	#[inline]
	fn push_none(&mut self, col: usize) -> Result<(), SdkError> {
		if self.passed[col] {
			return Err(passed_column(col));
		}
		self.cols[col].push_none();
		Ok(())
	}

	fn pass_through(
		&mut self,
		col: usize,
		input: &impl ColumnsView,
		name: &str,
		mask: &[bool],
	) -> Result<(), SdkError> {
		if self.passed[col] {
			return Err(passed_column(col));
		}
		if !self.cols[col].is_empty() {
			return Err(pass_after_push(col));
		}
		let PassSource::InProcess(view) = pass_source(input, name, mask)? else {
			return Err(foreign_source(col));
		};
		pass_kind(col, self.columns[col].1, column_data_to_type_code(view))?;
		let selected = shared_predicate(&BooleanBuffer::from(mask), mask.len())
			.filter(view.array())
			.map_err(|err| SdkError::Other(err.to_string()))?;
		self.cols[col].append_values(&ColumnView::try_from((&selected, view.field))?)?;
		self.passed[col] = true;
		Ok(())
	}
}
