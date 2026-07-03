// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, thread};

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_codec::ffi::cells::encode_decimal_cell;
use reifydb_value::value::{Value, decimal::Decimal, row_number::RowNumber, value_type::ValueType};

use crate::{
	error::SdkError,
	operator::{
		builder::{ColumnBuilder, ColumnsBuilder, CommittedColumn},
		context::ffi::FFIOperatorContext,
	},
};

pub struct DiffStart<'a> {
	inner: ColumnsBuilder<'a>,
}

impl<'a> DiffStart<'a> {
	pub(crate) fn new(ctx: &'a mut FFIOperatorContext) -> Self {
		Self {
			inner: ColumnsBuilder::new(ctx),
		}
	}

	pub fn insert<S, I>(self, row_number: RowNumber, fields: I) -> InsertDiff<'a>
	where
		S: Into<String>,
		I: IntoIterator<Item = (S, Value)>,
	{
		let mut diff = InsertDiff {
			inner: self.inner,
			schema: Vec::new(),
			rows: Vec::new(),
			disarmed: false,
		};
		let fields = collect_fields(fields);
		validate_row_or_panic(&mut diff.schema, &fields, "InsertDiff::insert");
		diff.rows.push(StagedRow {
			row_number,
			fields,
		});
		diff
	}

	pub fn update<S, I, J>(self, row_number: RowNumber, pre: I, post: J) -> UpdateDiff<'a>
	where
		S: Into<String>,
		I: IntoIterator<Item = (S, Value)>,
		J: IntoIterator<Item = (S, Value)>,
	{
		let mut diff = UpdateDiff {
			inner: self.inner,
			schema: Vec::new(),
			rows: Vec::new(),
			disarmed: false,
		};
		let pre = collect_fields(pre);
		let post = collect_fields(post);
		validate_row_or_panic(&mut diff.schema, &pre, "UpdateDiff::update pre");
		validate_row_or_panic(&mut diff.schema, &post, "UpdateDiff::update post");
		diff.rows.push(UpdateRow {
			row_number,
			pre,
			post,
		});
		diff
	}

	pub fn remove<S, I>(self, row_number: RowNumber, fields: I) -> RemoveDiff<'a>
	where
		S: Into<String>,
		I: IntoIterator<Item = (S, Value)>,
	{
		let mut diff = RemoveDiff {
			inner: self.inner,
			schema: Vec::new(),
			rows: Vec::new(),
			disarmed: false,
		};
		let fields = collect_fields(fields);
		validate_row_or_panic(&mut diff.schema, &fields, "RemoveDiff::remove");
		diff.rows.push(StagedRow {
			row_number,
			fields,
		});
		diff
	}
}

struct StagedRow {
	row_number: RowNumber,
	fields: Vec<(String, Value)>,
}

struct UpdateRow {
	row_number: RowNumber,
	pre: Vec<(String, Value)>,
	post: Vec<(String, Value)>,
}

pub struct InsertDiff<'a> {
	inner: ColumnsBuilder<'a>,
	schema: Vec<(String, ColumnTypeCode)>,
	rows: Vec<StagedRow>,
	disarmed: bool,
}

impl<'a> InsertDiff<'a> {
	pub fn insert<S, I>(mut self, row_number: RowNumber, fields: I) -> Self
	where
		S: Into<String>,
		I: IntoIterator<Item = (S, Value)>,
	{
		let fields = collect_fields(fields);
		validate_row_or_panic(&mut self.schema, &fields, "InsertDiff::insert");
		self.rows.push(StagedRow {
			row_number,
			fields,
		});
		self
	}

	pub fn try_finish(mut self) -> Result<(), SdkError> {
		self.disarmed = true;
		emit_insert(&mut self.inner, &self.schema, &self.rows)
	}
}

impl<'a> Drop for InsertDiff<'a> {
	fn drop(&mut self) {
		if self.disarmed {
			return;
		}
		if let Err(e) = emit_insert(&mut self.inner, &self.schema, &self.rows)
			&& !thread::panicking()
		{
			panic!("InsertDiff drop failed: {:?}", e);
		}
	}
}

pub struct UpdateDiff<'a> {
	inner: ColumnsBuilder<'a>,
	schema: Vec<(String, ColumnTypeCode)>,
	rows: Vec<UpdateRow>,
	disarmed: bool,
}

impl<'a> UpdateDiff<'a> {
	pub fn update<S, I, J>(mut self, row_number: RowNumber, pre: I, post: J) -> Self
	where
		S: Into<String>,
		I: IntoIterator<Item = (S, Value)>,
		J: IntoIterator<Item = (S, Value)>,
	{
		let pre = collect_fields(pre);
		let post = collect_fields(post);
		validate_row_or_panic(&mut self.schema, &pre, "UpdateDiff::update pre");
		validate_row_or_panic(&mut self.schema, &post, "UpdateDiff::update post");
		self.rows.push(UpdateRow {
			row_number,
			pre,
			post,
		});
		self
	}

	pub fn try_finish(mut self) -> Result<(), SdkError> {
		self.disarmed = true;
		emit_update(&mut self.inner, &self.schema, &self.rows)
	}
}

impl<'a> Drop for UpdateDiff<'a> {
	fn drop(&mut self) {
		if self.disarmed {
			return;
		}
		if let Err(e) = emit_update(&mut self.inner, &self.schema, &self.rows)
			&& !thread::panicking()
		{
			panic!("UpdateDiff drop failed: {:?}", e);
		}
	}
}

pub struct RemoveDiff<'a> {
	inner: ColumnsBuilder<'a>,
	schema: Vec<(String, ColumnTypeCode)>,
	rows: Vec<StagedRow>,
	disarmed: bool,
}

impl<'a> RemoveDiff<'a> {
	pub fn remove<S, I>(mut self, row_number: RowNumber, fields: I) -> Self
	where
		S: Into<String>,
		I: IntoIterator<Item = (S, Value)>,
	{
		let fields = collect_fields(fields);
		validate_row_or_panic(&mut self.schema, &fields, "RemoveDiff::remove");
		self.rows.push(StagedRow {
			row_number,
			fields,
		});
		self
	}

	pub fn try_finish(mut self) -> Result<(), SdkError> {
		self.disarmed = true;
		emit_remove(&mut self.inner, &self.schema, &self.rows)
	}
}

impl<'a> Drop for RemoveDiff<'a> {
	fn drop(&mut self) {
		if self.disarmed {
			return;
		}
		if let Err(e) = emit_remove(&mut self.inner, &self.schema, &self.rows)
			&& !thread::panicking()
		{
			panic!("RemoveDiff drop failed: {:?}", e);
		}
	}
}

fn collect_fields<S, I>(fields: I) -> Vec<(String, Value)>
where
	S: Into<String>,
	I: IntoIterator<Item = (S, Value)>,
{
	fields.into_iter().map(|(k, v)| (k.into(), v)).collect()
}

fn validate_row_or_panic(
	schema: &mut Vec<(String, ColumnTypeCode)>,
	fields: &[(String, Value)],
	context: &'static str,
) {
	if schema.is_empty() {
		infer_schema_from_first_row(schema, fields, context);
		return;
	}
	validate_row_against_schema(schema, fields, context);
}

#[inline]
fn infer_schema_from_first_row(
	schema: &mut Vec<(String, ColumnTypeCode)>,
	fields: &[(String, Value)],
	context: &'static str,
) {
	schema.reserve(fields.len());
	for (name, value) in fields {
		let type_code = match value_to_type_code(value) {
			Some(c) => c,
			None => panic!("{}: column {:?} has unsupported value type {:?}", context, name, value),
		};
		if schema.iter().any(|(n, _)| n == name) {
			panic!("{}: duplicate column name {:?}", context, name);
		}
		schema.push((name.clone(), type_code));
	}
}

#[inline]
fn validate_row_against_schema(schema: &[(String, ColumnTypeCode)], fields: &[(String, Value)], context: &'static str) {
	if fields.len() != schema.len() {
		panic!(
			"{}: row has {} fields, schema expects {} (schema: {:?})",
			context,
			fields.len(),
			schema.len(),
			schema.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
		);
	}
	let names: HashMap<&str, &Value> = fields.iter().map(|(n, v)| (n.as_str(), v)).collect();
	if names.len() != fields.len() {
		panic!("{}: duplicate column name within row", context);
	}
	for (name, expected) in schema.iter() {
		match names.get(name.as_str()) {
			None => panic!("{}: row missing column {:?}", context, name),
			Some(value) => {
				let observed = match value_to_type_code(value) {
					Some(c) => c,
					None => panic!(
						"{}: column {:?} has unsupported value type {:?}",
						context, name, value
					),
				};
				if observed != *expected && !matches!(value, Value::None { .. }) {
					panic!(
						"{}: column {:?} type mismatch (expected {:?}, got {:?})",
						context, name, expected, observed
					);
				}
			}
		}
	}
}

fn emit_insert(
	inner: &mut ColumnsBuilder<'_>,
	schema: &[(String, ColumnTypeCode)],
	rows: &[StagedRow],
) -> Result<(), SdkError> {
	if rows.is_empty() {
		return Ok(());
	}
	let row_count = rows.len();
	let row_numbers: Vec<RowNumber> = rows.iter().map(|r| r.row_number).collect();
	let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
	let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

	let columns = transpose(schema, &rows.iter().map(|r| &r.fields).collect::<Vec<_>>())?;
	let mut committed: Vec<CommittedColumn> = Vec::with_capacity(schema.len());
	for (i, (_, type_code)) in schema.iter().enumerate() {
		let col = inner.acquire(*type_code, row_count.max(1))?;
		committed.push(write_column(col, *type_code, &columns[i])?);
	}
	inner.emit_insert(&committed, &names_ref, &row_numbers)
}

fn emit_update(
	inner: &mut ColumnsBuilder<'_>,
	schema: &[(String, ColumnTypeCode)],
	rows: &[UpdateRow],
) -> Result<(), SdkError> {
	if rows.is_empty() {
		return Ok(());
	}
	let row_count = rows.len();
	let row_numbers: Vec<RowNumber> = rows.iter().map(|r| r.row_number).collect();
	let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
	let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

	let pre_cols = transpose(schema, &rows.iter().map(|r| &r.pre).collect::<Vec<_>>())?;
	let post_cols = transpose(schema, &rows.iter().map(|r| &r.post).collect::<Vec<_>>())?;
	let mut pre_committed: Vec<CommittedColumn> = Vec::with_capacity(schema.len());
	let mut post_committed: Vec<CommittedColumn> = Vec::with_capacity(schema.len());
	for (i, (_, type_code)) in schema.iter().enumerate() {
		let pre_col = inner.acquire(*type_code, row_count.max(1))?;
		pre_committed.push(write_column(pre_col, *type_code, &pre_cols[i])?);
		let post_col = inner.acquire(*type_code, row_count.max(1))?;
		post_committed.push(write_column(post_col, *type_code, &post_cols[i])?);
	}
	inner.emit_update(
		&pre_committed,
		&names_ref,
		row_count,
		&row_numbers,
		&post_committed,
		&names_ref,
		row_count,
		&row_numbers,
	)
}

fn emit_remove(
	inner: &mut ColumnsBuilder<'_>,
	schema: &[(String, ColumnTypeCode)],
	rows: &[StagedRow],
) -> Result<(), SdkError> {
	if rows.is_empty() {
		return Ok(());
	}
	let row_count = rows.len();
	let row_numbers: Vec<RowNumber> = rows.iter().map(|r| r.row_number).collect();
	let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
	let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

	let columns = transpose(schema, &rows.iter().map(|r| &r.fields).collect::<Vec<_>>())?;
	let mut committed: Vec<CommittedColumn> = Vec::with_capacity(schema.len());
	for (i, (_, type_code)) in schema.iter().enumerate() {
		let col = inner.acquire(*type_code, row_count.max(1))?;
		committed.push(write_column(col, *type_code, &columns[i])?);
	}
	inner.emit_remove(&committed, &names_ref, &row_numbers)
}

fn transpose(schema: &[(String, ColumnTypeCode)], rows: &[&Vec<(String, Value)>]) -> Result<Vec<Vec<Value>>, SdkError> {
	let mut columns: Vec<Vec<Value>> = (0..schema.len()).map(|_| Vec::with_capacity(rows.len())).collect();
	for row in rows {
		let lookup: HashMap<&str, &Value> = row.iter().map(|(n, v)| (n.as_str(), v)).collect();
		for (i, (name, _)) in schema.iter().enumerate() {
			match lookup.get(name.as_str()) {
				Some(value) => columns[i].push((*value).clone()),
				None => {
					return Err(SdkError::InvalidInput(format!(
						"transpose: row missing column {:?}",
						name
					)));
				}
			}
		}
	}
	Ok(columns)
}

fn write_column(
	col: ColumnBuilder<'_>,
	type_code: ColumnTypeCode,
	values: &[Value],
) -> Result<CommittedColumn, SdkError> {
	let defined: Vec<bool> = values.iter().map(|v| !matches!(v, Value::None { .. })).collect();
	let has_nulls = defined.iter().any(|d| !*d);
	if has_nulls {
		col.set_defined(&defined);
	}
	match type_code {
		ColumnTypeCode::Bool => {
			let buf: Vec<bool> = values.iter().map(value_to_bool).collect::<Result<_, _>>()?;
			col.write_bool(&buf)
		}
		ColumnTypeCode::Uint1 => {
			let buf: Vec<u8> = values.iter().map(value_to_u8).collect::<Result<_, _>>()?;
			col.write_u8(&buf)
		}
		ColumnTypeCode::Uint2 => {
			let buf: Vec<u16> = values.iter().map(value_to_u16).collect::<Result<_, _>>()?;
			col.write_u16(&buf)
		}
		ColumnTypeCode::Uint4 => {
			let buf: Vec<u32> = values.iter().map(value_to_u32).collect::<Result<_, _>>()?;
			col.write_u32(&buf)
		}
		ColumnTypeCode::Uint8 => {
			let buf: Vec<u64> = values.iter().map(value_to_u64).collect::<Result<_, _>>()?;
			col.write_u64(&buf)
		}
		ColumnTypeCode::Uint16 => {
			let buf: Vec<u128> = values.iter().map(value_to_u128).collect::<Result<_, _>>()?;
			col.write_u128(&buf)
		}
		ColumnTypeCode::Int1 => {
			let buf: Vec<i8> = values.iter().map(value_to_i8).collect::<Result<_, _>>()?;
			col.write_i8(&buf)
		}
		ColumnTypeCode::Int2 => {
			let buf: Vec<i16> = values.iter().map(value_to_i16).collect::<Result<_, _>>()?;
			col.write_i16(&buf)
		}
		ColumnTypeCode::Int4 => {
			let buf: Vec<i32> = values.iter().map(value_to_i32).collect::<Result<_, _>>()?;
			col.write_i32(&buf)
		}
		ColumnTypeCode::Int8 => {
			let buf: Vec<i64> = values.iter().map(value_to_i64).collect::<Result<_, _>>()?;
			col.write_i64(&buf)
		}
		ColumnTypeCode::Int16 => {
			let buf: Vec<i128> = values.iter().map(value_to_i128).collect::<Result<_, _>>()?;
			col.write_i128(&buf)
		}
		ColumnTypeCode::Float4 => {
			let buf: Vec<f32> = values.iter().map(value_to_f32).collect::<Result<_, _>>()?;
			col.write_f32(&buf)
		}
		ColumnTypeCode::Float8 => {
			let buf: Vec<f64> = values.iter().map(value_to_f64).collect::<Result<_, _>>()?;
			col.write_f64(&buf)
		}
		ColumnTypeCode::Utf8 => {
			let buf: Vec<String> = values.iter().map(value_to_utf8).collect::<Result<_, _>>()?;
			col.write_utf8(&buf)
		}
		ColumnTypeCode::Blob => {
			let buf: Vec<Vec<u8>> = values.iter().map(value_to_blob).collect::<Result<_, _>>()?;
			col.write_blob(&buf)
		}
		ColumnTypeCode::Decimal => write_decimal_column(col, values),
		other => Err(SdkError::NotImplemented(format!("emit: unsupported column type {:?}", other))),
	}
}

fn write_decimal_column(col: ColumnBuilder<'_>, values: &[Value]) -> Result<CommittedColumn, SdkError> {
	let mut serialized: Vec<Vec<u8>> = Vec::with_capacity(values.len());
	for v in values {
		let dec: Decimal = match v {
			Value::Decimal(d) => d.clone(),
			Value::Float4(f) => Decimal::from(f64::from(f32::from(*f))),
			Value::Float8(f) => Decimal::from(f64::from(*f)),
			Value::None {
				..
			} => Decimal::from_i64(0),
			_ => {
				return Err(SdkError::InvalidInput(format!(
					"emit decimal: expected Decimal, got {:?}",
					v
				)));
			}
		};
		let mut bytes = Vec::new();
		encode_decimal_cell(&dec, &mut bytes);
		serialized.push(bytes);
	}
	col.write_blob(&serialized)
}

fn value_to_type_code(value: &Value) -> Option<ColumnTypeCode> {
	let code = match value {
		Value::Boolean(_) => ColumnTypeCode::Bool,
		Value::Float4(_) => ColumnTypeCode::Float4,
		Value::Float8(_) => ColumnTypeCode::Float8,
		Value::Int1(_) => ColumnTypeCode::Int1,
		Value::Int2(_) => ColumnTypeCode::Int2,
		Value::Int4(_) => ColumnTypeCode::Int4,
		Value::Int8(_) => ColumnTypeCode::Int8,
		Value::Int16(_) => ColumnTypeCode::Int16,
		Value::Uint1(_) => ColumnTypeCode::Uint1,
		Value::Uint2(_) => ColumnTypeCode::Uint2,
		Value::Uint4(_) => ColumnTypeCode::Uint4,
		Value::Uint8(_) => ColumnTypeCode::Uint8,
		Value::Uint16(_) => ColumnTypeCode::Uint16,
		Value::Utf8(_) => ColumnTypeCode::Utf8,
		Value::Decimal(_) => ColumnTypeCode::Decimal,
		Value::Blob(_) => ColumnTypeCode::Blob,
		Value::None {
			inner,
		} => return type_to_column_code(inner.clone()),
		_ => return None,
	};
	Some(code)
}

fn type_to_column_code(ty: ValueType) -> Option<ColumnTypeCode> {
	let code = match ty {
		ValueType::Boolean => ColumnTypeCode::Bool,
		ValueType::Float4 => ColumnTypeCode::Float4,
		ValueType::Float8 => ColumnTypeCode::Float8,
		ValueType::Int1 => ColumnTypeCode::Int1,
		ValueType::Int2 => ColumnTypeCode::Int2,
		ValueType::Int4 => ColumnTypeCode::Int4,
		ValueType::Int8 => ColumnTypeCode::Int8,
		ValueType::Int16 => ColumnTypeCode::Int16,
		ValueType::Uint1 => ColumnTypeCode::Uint1,
		ValueType::Uint2 => ColumnTypeCode::Uint2,
		ValueType::Uint4 => ColumnTypeCode::Uint4,
		ValueType::Uint8 => ColumnTypeCode::Uint8,
		ValueType::Uint16 => ColumnTypeCode::Uint16,
		ValueType::Utf8 => ColumnTypeCode::Utf8,
		ValueType::Decimal => ColumnTypeCode::Decimal,
		ValueType::Blob => ColumnTypeCode::Blob,
		_ => return Option::None,
	};
	Some(code)
}

fn type_mismatch_err(name: &str, value: &Value) -> SdkError {
	SdkError::InvalidInput(format!("emit: column {} type mismatch (got {:?})", name, value))
}

fn value_to_bool(v: &Value) -> Result<bool, SdkError> {
	match v {
		Value::Boolean(b) => Ok(*b),
		Value::None {
			..
		} => Ok(false),
		_ => Err(type_mismatch_err("bool", v)),
	}
}

macro_rules! value_to_int {
	($name:ident, $variant:ident, $ty:ty) => {
		fn $name(v: &Value) -> Result<$ty, SdkError> {
			match v {
				Value::$variant(x) => Ok(*x),
				Value::None {
					..
				} => Ok(<$ty as Default>::default()),
				_ => Err(type_mismatch_err(stringify!($variant), v)),
			}
		}
	};
}

value_to_int!(value_to_u8, Uint1, u8);
value_to_int!(value_to_u16, Uint2, u16);
value_to_int!(value_to_u32, Uint4, u32);
value_to_int!(value_to_u64, Uint8, u64);
value_to_int!(value_to_u128, Uint16, u128);
value_to_int!(value_to_i8, Int1, i8);
value_to_int!(value_to_i16, Int2, i16);
value_to_int!(value_to_i32, Int4, i32);
value_to_int!(value_to_i64, Int8, i64);
value_to_int!(value_to_i128, Int16, i128);

fn value_to_f32(v: &Value) -> Result<f32, SdkError> {
	match v {
		Value::Float4(f) => Ok(f32::from(*f)),
		Value::None {
			..
		} => Ok(0.0),
		_ => Err(type_mismatch_err("Float4", v)),
	}
}

fn value_to_f64(v: &Value) -> Result<f64, SdkError> {
	match v {
		Value::Float8(f) => Ok(f64::from(*f)),
		Value::None {
			..
		} => Ok(0.0),
		_ => Err(type_mismatch_err("Float8", v)),
	}
}

fn value_to_utf8(v: &Value) -> Result<String, SdkError> {
	match v {
		Value::Utf8(s) => Ok(s.clone()),
		Value::None {
			..
		} => Ok(String::new()),
		_ => Err(type_mismatch_err("Utf8", v)),
	}
}

fn value_to_blob(v: &Value) -> Result<Vec<u8>, SdkError> {
	match v {
		Value::Blob(b) => Ok(b.as_ref().to_vec()),
		Value::None {
			..
		} => Ok(Vec::new()),
		_ => Err(type_mismatch_err("Blob", v)),
	}
}
