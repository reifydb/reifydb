// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use reifydb_extension::block_encoding::{BlockEncoding, BoundBlockEncoding, error::BlockEncodingError};
use reifydb_value::{Result, value::Value};

pub const DELTA: &str = "delta";

pub struct DeltaEncoding {
	version: u32,
}

impl DeltaEncoding {
	pub fn new() -> Self {
		Self::with_version(1)
	}

	pub fn with_version(version: u32) -> Self {
		Self {
			version,
		}
	}
}

impl Default for DeltaEncoding {
	fn default() -> Self {
		Self::new()
	}
}

impl BlockEncoding for DeltaEncoding {
	fn version(&self) -> u32 {
		self.version
	}

	fn bind(&self, user: &[FieldRef], config: &[(String, Value)]) -> Result<Box<dyn BoundBlockEncoding>> {
		let column = match config {
			[(key, Value::Utf8(column))] if key == "column" => column.clone(),
			_ => return Err(invalid(format!("expects exactly `column`, got {config:?}")).into()),
		};
		let field = user
			.iter()
			.find(|field| field.name() == &column)
			.ok_or_else(|| invalid(format!("`{column}` is not a column of the series")))?;
		if field.data_type() != &DataType::Int64 || field.is_nullable() {
			return Err(invalid(format!("`{column}` must be int8, found {}", field.data_type())).into());
		}
		let stored_name = format!("{column}_delta");
		if user.iter().any(|field| field.name() == &stored_name) {
			return Err(invalid(format!("`{stored_name}` is already a column of the series")).into());
		}
		let stored = user
			.iter()
			.map(|field| match field.name() == &column {
				true => Arc::new(Field::new(&stored_name, DataType::Int64, false)),
				false => Arc::clone(field),
			})
			.collect();
		Ok(Box::new(BoundDelta {
			version: self.version,
			column,
			stored_name,
			user: user.to_vec(),
			stored,
		}))
	}
}

struct BoundDelta {
	version: u32,
	column: String,
	stored_name: String,
	user: Vec<FieldRef>,
	stored: Vec<FieldRef>,
}

impl BoundBlockEncoding for BoundDelta {
	fn stored_fields(&self) -> Vec<FieldRef> {
		self.stored.clone()
	}

	fn encode(&self, user: RecordBatch) -> Result<RecordBatch> {
		reshape(&user, &self.column, &self.stored, encode_deltas)
	}

	fn decode(&self, version: u32, stored: RecordBatch) -> Result<RecordBatch> {
		if version == 0 || version > self.version {
			return Err(
				invalid(format!("cannot decode version {version}, knows 1 to {}", self.version)).into()
			);
		}
		reshape(&stored, &self.stored_name, &self.user, decode_deltas)
	}
}

fn invalid(reason: String) -> BlockEncodingError {
	BlockEncodingError::InvalidConfig {
		name: DELTA.to_string(),
		reason,
	}
}

fn reshape(batch: &RecordBatch, from: &str, fields: &[FieldRef], step: fn(&[i64]) -> Vec<i64>) -> Result<RecordBatch> {
	let source = batch
		.column_by_name(from)
		.and_then(|array| array.as_any().downcast_ref::<Int64Array>())
		.ok_or_else(|| invalid(format!("block has no int8 column `{from}`")))?;
	let changed: ArrayRef = Arc::new(Int64Array::from(step(source.values())));
	let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
	let index = batch.schema().index_of(from).map_err(|e| invalid(e.to_string()))?;
	columns[index] = changed;
	RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns).map_err(|e| invalid(e.to_string()).into())
}

fn encode_deltas(values: &[i64]) -> Vec<i64> {
	(0..values.len())
		.map(|i| match i {
			0 => values[0],
			_ => values[i].wrapping_sub(values[i - 1]),
		})
		.collect()
}

fn decode_deltas(stored: &[i64]) -> Vec<i64> {
	let mut out: Vec<i64> = Vec::with_capacity(stored.len());
	for (i, &step) in stored.iter().enumerate() {
		let value = match i {
			0 => step,
			_ => out[i - 1].wrapping_add(step),
		};
		out.push(value);
	}
	out
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt64Array};
	use arrow_schema::{DataType, Field, FieldRef, Schema};
	use reifydb_extension::block_encoding::{BlockEncoding, BoundBlockEncoding};
	use reifydb_value::value::Value;

	use super::DeltaEncoding;

	fn fields(extra: &[(&str, DataType, bool)]) -> Vec<FieldRef> {
		let mut fields: Vec<FieldRef> = vec![Arc::new(Field::new("k", DataType::UInt64, false))];
		fields.extend(extra
			.iter()
			.map(|(name, ty, nullable)| Arc::new(Field::new(*name, ty.clone(), *nullable))));
		fields
	}

	fn config(column: &str) -> Vec<(String, Value)> {
		vec![("column".to_string(), Value::Utf8(column.to_string()))]
	}

	fn bound() -> Box<dyn BoundBlockEncoding> {
		DeltaEncoding::new().bind(&fields(&[("value", DataType::Int64, false)]), &config("value")).unwrap()
	}

	fn batch(keys: Vec<u64>, values: Vec<i64>) -> RecordBatch {
		let columns: Vec<ArrayRef> =
			vec![Arc::new(UInt64Array::from(keys)), Arc::new(Int64Array::from(values))];
		RecordBatch::try_new(Arc::new(Schema::new(fields(&[("value", DataType::Int64, false)]))), columns)
			.unwrap()
	}

	fn int64(batch: &RecordBatch, index: usize) -> Vec<i64> {
		batch.column(index).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
	}

	fn bind_error(user: &[FieldRef], column: &str) -> String {
		let diagnostic =
			DeltaEncoding::new().bind(user, &config(column)).err().expect("bind must fail").diagnostic();
		assert_eq!(diagnostic.code, "BENC_008", "{diagnostic:?}");
		diagnostic.message
	}

	#[test]
	fn delta_round_trips_values_with_overflowing_steps() {
		// Checked or saturating steps would fail or clamp on the jump from MAX to MIN.
		let user = batch(vec![3, 2, 1], vec![i64::MAX, i64::MIN, 7]);
		let bound = bound();
		let stored = bound.encode(user.clone()).unwrap();
		assert_eq!(bound.decode(1, stored).unwrap(), user);
	}

	#[test]
	fn delta_stores_differences_after_the_first_row() {
		// An encode that returns its input leaves every row-spanning decode test blind.
		let stored = bound().encode(batch(vec![1, 2, 3], vec![10, 13, 9])).unwrap();
		assert_eq!(int64(&stored, 1), vec![10, 3, -4]);
	}

	#[test]
	fn delta_leaves_every_other_column_untouched() {
		// A pass-through column that changes breaks the key check at seal.
		let user = batch(vec![5, 9, 11], vec![10, 13, 9]);
		let stored = bound().encode(user.clone()).unwrap();
		assert_eq!(stored.column(0), user.column(0));
	}

	#[test]
	fn delta_rejects_a_version_it_never_wrote() {
		// Decoding a newer layout with old rules returns wrong values silently.
		let bound = DeltaEncoding::with_version(1)
			.bind(&fields(&[("value", DataType::Int64, false)]), &config("value"))
			.unwrap();
		let stored = bound.encode(batch(vec![1], vec![1])).unwrap();
		assert!(bound.decode(2, stored).is_err());
	}

	#[test]
	fn delta_bind_rejects_missing_column_wrong_type_and_optional() {
		// Each missing check lets CREATE accept a series the encoding cannot encode.
		let user = fields(&[
			("value", DataType::Int64, false),
			("f", DataType::Float64, false),
			("o", DataType::Int64, true),
		]);
		for column in ["nope", "f", "o"] {
			let message = bind_error(&user, column);
			assert!(message.contains(&format!("`{column}`")), "{message}");
		}
		assert!(DeltaEncoding::new().bind(&user, &config("value")).is_ok());
	}

	#[test]
	fn delta_bind_rejects_a_user_column_named_like_its_stored_column() {
		// A collision gives the stored block two fields with one name.
		let user = fields(&[("value", DataType::Int64, false), ("value_delta", DataType::Int64, false)]);
		let message = bind_error(&user, "value");
		assert!(message.contains("value_delta"), "{message}");
	}

	#[test]
	fn delta_stores_the_column_as_column_delta() {
		// A stored name equal to the user name lets stored deltas leak into user frames unnoticed.
		let stored = bound().encode(batch(vec![1, 2, 3], vec![10, 13, 9])).unwrap();
		let names: Vec<String> = stored.schema().fields().iter().map(|f| f.name().to_string()).collect();
		assert_eq!(names, vec!["k", "value_delta"]);
	}
}
