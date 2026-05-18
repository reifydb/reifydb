// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::key::EncodedKey,
	interface::resolved::ResolvedSeries,
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, datetime::DateTime, row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use crate::{
	Result,
	vm::{
		instruction::dml::shape::get_or_create_series_shape,
		volcano::query::{QueryContext, QueryNode},
	},
};

pub struct SeriesScanNode {
	series: ResolvedSeries,
	key_range_start: Option<u64>,
	key_range_end: Option<u64>,
	variant_tag: Option<u8>,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl SeriesScanNode {
	pub fn new(
		series: ResolvedSeries,
		key_range_start: Option<u64>,
		key_range_end: Option<u64>,
		variant_tag: Option<u8>,
		context: Arc<QueryContext>,
	) -> Result<Self> {
		let mut columns = vec![Fragment::internal(series.def().key.column())];
		if series.def().tag.is_some() {
			columns.push(Fragment::internal("tag"));
		}
		for col in series.columns() {
			columns.push(Fragment::internal(&col.name));
		}
		let headers = ColumnHeaders {
			columns,
		};

		Ok(Self {
			series,
			key_range_start,
			key_range_end,
			variant_tag,
			context: Some(context),
			headers,
			last_key: None,
			exhausted: false,
		})
	}
}

impl QueryNode for SeriesScanNode {
	#[instrument(name = "volcano::scan::series::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::scan::series::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "SeriesScanNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let series = self.series.def();
		let has_tag = series.tag.is_some();

		let range = SeriesRowKeyRange::scan_range(
			series.id,
			self.variant_tag,
			self.key_range_start,
			self.key_range_end,
			self.last_key.as_ref(),
		);

		let mut key_values: Vec<u64> = Vec::new();
		let mut tags: Vec<u8> = Vec::new();
		let mut sequences: Vec<u64> = Vec::new();
		let mut created_at_values: Vec<DateTime> = Vec::new();
		let mut updated_at_values: Vec<DateTime> = Vec::new();
		let mut data_rows: Vec<Vec<Value>> = Vec::new();
		let mut new_last_key = None;

		let read_shape = get_or_create_series_shape(&stored_ctx.services.catalog, self.series.def(), rx)?;

		let mut stream = rx.range(range, batch_size as usize)?;
		let mut count = 0;

		for entry in stream.by_ref() {
			let entry = entry?;

			if let Some(key) = SeriesRowKey::decode(&entry.key) {
				key_values.push(key.key);
				sequences.push(key.sequence);
				created_at_values.push(DateTime::from_nanos(entry.row.created_at_nanos()));
				updated_at_values.push(DateTime::from_nanos(entry.row.updated_at_nanos()));
				if has_tag {
					tags.push(key.variant_tag.unwrap_or(0));
				}

				let mut values = Vec::with_capacity(series.data_columns().count());
				for (i, _) in series.data_columns().enumerate() {
					values.push(read_shape.get_value(&entry.row, i + 1));
				}
				data_rows.push(values);

				new_last_key = Some(entry.key);
				count += 1;
				if count >= batch_size as usize {
					break;
				}
			}
		}

		drop(stream);

		if key_values.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				let key_type = series
					.columns
					.iter()
					.find(|c| c.name == series.key.column())
					.map(|c| c.constraint.get_type())
					.unwrap_or(Type::Int8);
				let mut result_columns = Vec::new();
				result_columns.push(ColumnWithName {
					name: Fragment::internal(series.key.column()),
					data: ColumnBuffer::none_typed(key_type, 0),
				});
				if has_tag {
					result_columns.push(ColumnWithName {
						name: Fragment::internal("tag"),
						data: ColumnBuffer::none_typed(Type::Uint1, 0),
					});
				}
				for col_def in series.data_columns() {
					result_columns.push(ColumnWithName {
						name: Fragment::internal(&col_def.name),
						data: ColumnBuffer::none_typed(col_def.constraint.get_type(), 0),
					});
				}
				return Ok(Some(Columns::new(result_columns)));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		let mut result_columns = Vec::new();

		result_columns.push(ColumnWithName::new(
			Fragment::internal(series.key.column()),
			series.key_column_data(key_values),
		));

		if has_tag {
			result_columns.push(ColumnWithName::new(Fragment::internal("tag"), ColumnBuffer::uint1(tags)));
		}

		for (col_idx, col_def) in series.data_columns().enumerate() {
			let col_type = col_def.constraint.get_type();
			let col_values: Vec<Value> = data_rows
				.iter()
				.map(|row| row.get(col_idx).cloned().unwrap_or(Value::none()))
				.collect();

			result_columns.push(build_data_column(&col_def.name, &col_values, col_type)?);
		}

		let row_numbers: Vec<RowNumber> = sequences.into_iter().map(RowNumber::from).collect();
		Ok(Some(Columns::with_system_columns(
			result_columns,
			row_numbers,
			created_at_values,
			updated_at_values,
		)))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

pub(crate) fn build_data_column(name: &str, values: &[Value], col_type: Type) -> Result<ColumnWithName> {
	let data = match col_type {
		Type::Boolean => {
			let vals: Vec<bool> = values
				.iter()
				.map(|v| match v {
					Value::Boolean(b) => *b,
					_ => false,
				})
				.collect();
			ColumnBuffer::bool(vals)
		}
		Type::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int1(vals)
		}
		Type::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int2(vals)
		}
		Type::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int4(vals)
		}
		Type::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int8(vals)
		}
		Type::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint8(vals)
		}
		Type::Float4 => {
			let vals: Vec<f32> = values
				.iter()
				.map(|v| match v {
					Value::Float4(n) => n.value(),
					_ => 0.0,
				})
				.collect();
			ColumnBuffer::float4(vals)
		}
		Type::Float8 => {
			let vals: Vec<f64> = values
				.iter()
				.map(|v| match v {
					Value::Float8(n) => n.value(),
					_ => 0.0,
				})
				.collect();
			ColumnBuffer::float8(vals)
		}
		Type::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => String::new(),
				})
				.collect();
			ColumnBuffer::utf8(vals)
		}
		_ => {
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnBuffer::utf8(vals)
		}
	};

	Ok(ColumnWithName {
		name: Fragment::internal(name),
		data,
	})
}
