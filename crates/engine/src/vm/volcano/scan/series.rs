// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::key::EncodedKey,
	interface::resolved::ResolvedSeries,
	internal_error,
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};
use tracing::instrument;

use crate::vm::volcano::query::{QueryContext, QueryNode};

pub struct SeriesScanNode {
	series: ResolvedSeries,
	time_range_start: Option<i64>,
	time_range_end: Option<i64>,
	variant_tag: Option<u8>,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl SeriesScanNode {
	pub fn new(
		series: ResolvedSeries,
		time_range_start: Option<i64>,
		time_range_end: Option<i64>,
		variant_tag: Option<u8>,
		context: Arc<QueryContext>,
	) -> crate::Result<Self> {
		// Build headers: timestamp, optional tag, then data columns
		let mut columns = vec![Fragment::internal("timestamp")];
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
			time_range_start,
			time_range_end,
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
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> crate::Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::scan::series::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "SeriesScanNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let series_def = self.series.def();
		let has_tag = series_def.tag.is_some();

		// Create scan range
		let range = SeriesRowKeyRange::scan_range(
			series_def.id,
			self.variant_tag,
			self.time_range_start,
			self.time_range_end,
			self.last_key.as_ref(),
		);

		let mut timestamps: Vec<i64> = Vec::new();
		let mut tags: Vec<u8> = Vec::new();
		let mut data_rows: Vec<Vec<Value>> = Vec::new();
		let mut new_last_key = None;

		let mut stream = rx.range(range, batch_size as usize)?;
		let mut count = 0;

		while let Some(entry) = stream.next() {
			let entry = entry?;

			// Decode the key to get timestamp and optional tag
			if let Some(key) = SeriesRowKey::decode(&entry.key) {
				timestamps.push(key.timestamp);
				if has_tag {
					tags.push(key.variant_tag.unwrap_or(0));
				}

				// Decode data columns from value
				let values: Vec<Value> = postcard::from_bytes(&entry.values).map_err(|e| {
					internal_error!("Failed to deserialize series row values: {}", e)
				})?;
				data_rows.push(values);

				new_last_key = Some(entry.key);
				count += 1;
				if count >= batch_size as usize {
					break;
				}
			}
		}

		drop(stream);

		if timestamps.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		self.last_key = new_last_key;

		// Build output columns
		let mut result_columns = Vec::new();

		// Timestamp column (always Int8)
		result_columns.push(Column {
			name: Fragment::internal("timestamp"),
			data: ColumnData::int8(timestamps),
		});

		// Tag column (Uint1) if present
		if has_tag {
			result_columns.push(Column {
				name: Fragment::internal("tag"),
				data: ColumnData::uint1(tags),
			});
		}

		// Data columns
		for (col_idx, col_def) in series_def.columns.iter().enumerate() {
			let col_type = col_def.constraint.get_type();
			let col_values: Vec<Value> = data_rows
				.iter()
				.map(|row| row.get(col_idx).cloned().unwrap_or(Value::none()))
				.collect();

			result_columns.push(build_data_column(&col_def.name, &col_values, col_type)?);
		}

		Ok(Some(Columns::new(result_columns)))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

pub(crate) fn build_data_column(name: &str, values: &[Value], col_type: Type) -> crate::Result<Column> {
	let data = match col_type {
		Type::Boolean => {
			let vals: Vec<bool> = values
				.iter()
				.map(|v| match v {
					Value::Boolean(b) => *b,
					_ => false,
				})
				.collect();
			ColumnData::bool(vals)
		}
		Type::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int1(vals)
		}
		Type::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int2(vals)
		}
		Type::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int4(vals)
		}
		Type::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int8(vals)
		}
		Type::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint8(vals)
		}
		Type::Float4 => {
			let vals: Vec<f32> = values
				.iter()
				.map(|v| match v {
					Value::Float4(n) => n.value(),
					_ => 0.0,
				})
				.collect();
			ColumnData::float4(vals)
		}
		Type::Float8 => {
			let vals: Vec<f64> = values
				.iter()
				.map(|v| match v {
					Value::Float8(n) => n.value(),
					_ => 0.0,
				})
				.collect();
			ColumnData::float8(vals)
		}
		Type::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => String::new(),
				})
				.collect();
			ColumnData::utf8(vals)
		}
		_ => {
			// Fallback: convert to string representation
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnData::utf8(vals)
		}
	};

	Ok(Column {
		name: Fragment::internal(name),
		data,
	})
}
