// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb::{
	arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt64Array},
	arrow_schema::{DataType, Field, FieldRef, Schema},
	extension::block_encoding::{BlockEncoding, BoundBlockEncoding, error::BlockEncodingError},
	value::{Result, value::Value},
};

pub const OHLCV: &str = "ohlcv";

const PRICES: [&str; 4] = ["open", "high", "low", "close"];
const DERIVED: [&str; 3] = ["gap", "upper", "lower"];

pub struct OhlcvEncoding;

impl BlockEncoding for OhlcvEncoding {
	fn version(&self) -> u32 {
		1
	}

	fn bind(&self, user: &[FieldRef], config: &[(String, Value)]) -> Result<Box<dyn BoundBlockEncoding>> {
		if config.len() != PRICES.len() {
			return Err(invalid(format!("expects exactly open, high, low, close, got {config:?}")).into());
		}
		let mut columns = Vec::with_capacity(PRICES.len());
		for price in PRICES {
			let column = match config.iter().find(|(key, _)| key == price) {
				Some((_, Value::Utf8(column))) => column.clone(),
				_ => {
					return Err(
						invalid(format!("`{price}` must name a column, got {config:?}")).into()
					);
				}
			};
			let field = user
				.iter()
				.find(|field| field.name() == &column)
				.ok_or_else(|| invalid(format!("`{column}` is not a column of the series")))?;
			if field.data_type() != &DataType::Int64 || field.is_nullable() {
				return Err(
					invalid(format!("`{column}` must be int8, found {}", field.data_type())).into()
				);
			}
			columns.push(column);
		}
		if let Some(clash) = DERIVED.iter().find(|name| user.iter().any(|field| field.name() == *name)) {
			return Err(invalid(format!("`{clash}` is already a column of the series")).into());
		}
		let [open, high, low, close] =
			<[String; 4]>::try_from(columns).map_err(|_| invalid("four prices".into()))?;
		let mut stored = Vec::with_capacity(user.len() + 1);
		for field in user {
			if [&high, &low, &close].contains(&field.name()) {
				continue;
			}
			stored.push(Arc::clone(field));
			if field.name() == &open {
				stored.push(Arc::new(Field::new("gap", DataType::Int64, false)));
				stored.push(Arc::new(Field::new("upper", DataType::UInt64, false)));
				stored.push(Arc::new(Field::new("lower", DataType::UInt64, false)));
			}
		}
		Ok(Box::new(BoundOhlcv {
			open,
			high,
			low,
			close,
			user: user.to_vec(),
			stored,
		}))
	}
}

struct BoundOhlcv {
	open: String,
	high: String,
	low: String,
	close: String,
	user: Vec<FieldRef>,
	stored: Vec<FieldRef>,
}

impl BoundBlockEncoding for BoundOhlcv {
	fn stored_fields(&self) -> Vec<FieldRef> {
		self.stored.clone()
	}

	fn encode(&self, user: RecordBatch) -> Result<RecordBatch> {
		let open = int64(&user, &self.open)?;
		let high = int64(&user, &self.high)?;
		let low = int64(&user, &self.low)?;
		let close = int64(&user, &self.close)?;
		let rows = user.num_rows();
		let mut gap = Vec::with_capacity(rows);
		let mut upper = Vec::with_capacity(rows);
		let mut lower = Vec::with_capacity(rows);
		for i in 0..rows {
			let next_open = open[i.saturating_sub(1)];
			gap.push(close[i]
				.checked_sub(next_open)
				.ok_or_else(|| invalid(format!("gap overflows at row {i}")))?);
			let body_high = open[i].max(close[i]);
			let body_low = open[i].min(close[i]);
			let above = high[i]
				.checked_sub(body_high)
				.ok_or_else(|| invalid(format!("upper overflows at row {i}")))?;
			let below = body_low
				.checked_sub(low[i])
				.ok_or_else(|| invalid(format!("lower overflows at row {i}")))?;
			upper.push(u64::try_from(above).map_err(|_| invalid(format!("high below body at row {i}")))?);
			lower.push(u64::try_from(below).map_err(|_| invalid(format!("low above body at row {i}")))?);
		}
		let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.stored.len());
		for field in &self.stored {
			let name = field.name().as_str();
			columns.push(match name {
				"gap" => Arc::new(Int64Array::from(gap.clone())),
				"upper" => Arc::new(UInt64Array::from(upper.clone())),
				"lower" => Arc::new(UInt64Array::from(lower.clone())),
				_ => copied(&user, name)?,
			});
		}
		assemble(&self.stored, columns)
	}

	fn decode(&self, version: u32, stored: RecordBatch) -> Result<RecordBatch> {
		if version != 1 {
			return Err(invalid(format!("cannot decode version {version}, knows 1")).into());
		}
		let open = int64(&stored, &self.open)?;
		let gap = int64(&stored, "gap")?;
		let upper = uint64(&stored, "upper")?;
		let lower = uint64(&stored, "lower")?;
		let rows = stored.num_rows();
		let mut high = Vec::with_capacity(rows);
		let mut low = Vec::with_capacity(rows);
		let mut close = Vec::with_capacity(rows);
		for i in 0..rows {
			let next_open = open[i.saturating_sub(1)];
			let c = next_open
				.checked_add(gap[i])
				.ok_or_else(|| invalid(format!("close overflows at row {i}")))?;
			let above =
				i64::try_from(upper[i]).map_err(|_| invalid(format!("upper overflows at row {i}")))?;
			let below =
				i64::try_from(lower[i]).map_err(|_| invalid(format!("lower overflows at row {i}")))?;
			high.push(open[i]
				.max(c)
				.checked_add(above)
				.ok_or_else(|| invalid(format!("high overflows at row {i}")))?);
			low.push(open[i]
				.min(c)
				.checked_sub(below)
				.ok_or_else(|| invalid(format!("low overflows at row {i}")))?);
			close.push(c);
		}
		let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.user.len());
		for field in &self.user {
			let name = field.name();
			columns.push(match name {
				_ if *name == self.high => Arc::new(Int64Array::from(high.clone())),
				_ if *name == self.low => Arc::new(Int64Array::from(low.clone())),
				_ if *name == self.close => Arc::new(Int64Array::from(close.clone())),
				_ => copied(&stored, name)?,
			});
		}
		assemble(&self.user, columns)
	}
}

fn invalid(reason: String) -> BlockEncodingError {
	BlockEncodingError::InvalidConfig {
		name: OHLCV.to_string(),
		reason,
	}
}

fn copied(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
	Ok(Arc::clone(batch.column_by_name(name).ok_or_else(|| invalid(format!("block has no column `{name}`")))?))
}

fn int64(batch: &RecordBatch, name: &str) -> Result<Vec<i64>> {
	let array = copied(batch, name)?;
	let values =
		array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| invalid(format!("`{name}` is not int8")))?;
	Ok(values.values().to_vec())
}

fn uint64(batch: &RecordBatch, name: &str) -> Result<Vec<u64>> {
	let array = copied(batch, name)?;
	let values = array
		.as_any()
		.downcast_ref::<UInt64Array>()
		.ok_or_else(|| invalid(format!("`{name}` is not uint8")))?;
	Ok(values.values().to_vec())
}

fn assemble(fields: &[FieldRef], columns: Vec<ArrayRef>) -> Result<RecordBatch> {
	RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns).map_err(|e| invalid(e.to_string()).into())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb::{
		arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt64Array},
		arrow_schema::{DataType, Field, FieldRef, Schema},
		extension::block_encoding::BlockEncoding,
		value::value::Value,
	};

	use super::{OhlcvEncoding, PRICES};
	use crate::equity::{EquityConfig, generate};

	fn config() -> Vec<(String, Value)> {
		PRICES.iter().map(|price| (price.to_string(), Value::Utf8(price.to_string()))).collect()
	}

	fn fields() -> Vec<FieldRef> {
		let mut fields: Vec<FieldRef> = vec![Arc::new(Field::new("ts", DataType::Int64, false))];
		for price in PRICES {
			fields.push(Arc::new(Field::new(price, DataType::Int64, false)));
		}
		fields.push(Arc::new(Field::new("volume", DataType::UInt64, false)));
		fields
	}

	fn bars_desc() -> RecordBatch {
		let bars = generate(&EquityConfig {
			days: 1,
			..EquityConfig::default()
		});
		let desc = |values: &[i64]| -> ArrayRef {
			Arc::new(Int64Array::from_iter_values(values.iter().rev().copied()))
		};
		let columns: Vec<ArrayRef> = vec![
			desc(&bars.ts),
			desc(&bars.open),
			desc(&bars.high),
			desc(&bars.low),
			desc(&bars.close),
			Arc::new(UInt64Array::from_iter_values(bars.volume.iter().rev().copied())),
		];
		RecordBatch::try_new(Arc::new(Schema::new(fields())), columns).unwrap()
	}

	#[test]
	fn ohlcv_round_trips_a_block_in_key_desc_order() {
		// Blocks seal in key descending order; a segment cut off by one row corrupts every later price.
		let bound = OhlcvEncoding.bind(&fields(), &config()).unwrap();
		let block = bars_desc();
		assert!(block.num_rows() > 1, "the fixture must span several rows");
		let stored = bound.encode(block.clone()).unwrap();
		assert_eq!(bound.decode(1, stored).unwrap(), block);
	}

	#[test]
	fn ohlcv_stores_open_gap_upper_lower_in_place_of_the_prices() {
		// Storing high, low or close next to the derived columns would keep the bytes the encoding exists to
		// remove.
		let bound = OhlcvEncoding.bind(&fields(), &config()).unwrap();
		let names: Vec<_> = bound.stored_fields().iter().map(|field| field.name().clone()).collect();
		assert_eq!(names, vec!["ts", "open", "gap", "upper", "lower", "volume"]);
		let stored = bound.encode(bars_desc()).unwrap();
		let stored_names: Vec<_> = stored.schema().fields().iter().map(|field| field.name().clone()).collect();
		assert_eq!(stored_names, names);
	}

	fn bars(rows: &[[i64; 5]]) -> RecordBatch {
		// Rows are ts, open, high, low, close in stored order, newest first, as a seal writes them.
		let column =
			|c: usize| -> ArrayRef { Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r[c]))) };
		let volume: ArrayRef = Arc::new(UInt64Array::from_iter_values(rows.iter().map(|_| 1)));
		let columns = vec![column(0), column(1), column(2), column(3), column(4), volume];
		RecordBatch::try_new(Arc::new(Schema::new(fields())), columns).unwrap()
	}

	fn stored_gap(stored: &RecordBatch) -> Vec<i64> {
		stored.column_by_name("gap").unwrap().as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
	}

	#[test]
	fn ohlcv_gap_is_zero_where_a_bar_opens_at_the_previous_close() {
		// The encoding only saves bytes if a chained series stores gap as zeros; walking the chain the wrong
		// way makes every gap a bar-to-bar move and brings the noise back.
		let bound = OhlcvEncoding.bind(&fields(), &config()).unwrap();
		let block = bars(&[[300, 102, 104, 101, 103], [200, 101, 103, 100, 102], [100, 100, 102, 99, 101]]);
		let stored = bound.encode(block.clone()).unwrap();
		assert_eq!(stored_gap(&stored), vec![1, 0, 0], "only the newest bar may carry a nonzero gap");
		assert_eq!(bound.decode(1, stored).unwrap(), block);
	}

	#[test]
	fn ohlcv_round_trips_a_bar_that_opens_away_from_the_previous_close() {
		// Real bars do not always open at the previous close; dropping the gap correction would return a
		// close, high and low that were never written.
		let bound = OhlcvEncoding.bind(&fields(), &config()).unwrap();
		let block = bars(&[[300, 102, 104, 101, 103], [200, 101, 106, 100, 105], [100, 100, 102, 99, 101]]);
		let stored = bound.encode(block.clone()).unwrap();
		assert_eq!(stored_gap(&stored), vec![1, 3, 0], "the jump must be kept as a nonzero gap");
		assert_eq!(bound.decode(1, stored).unwrap(), block);
	}

	#[test]
	fn ohlcv_bind_rejects_a_decimal_price() {
		// A decimal price would be read as raw int8 words and decode to garbage.
		let mut fields = fields();
		fields[1] = Arc::new(Field::new("open", DataType::Decimal128(38, 4), false));
		let err = OhlcvEncoding.bind(&fields, &config()).err().expect("bind must reject a decimal open");
		assert_eq!(err.0.code, "BENC_008");
		assert!(err.0.message.contains("open"), "the error must name the column: {}", err.0.message);
	}
}
