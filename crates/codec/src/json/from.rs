// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	fragment::Fragment,
	util::{bitvec::BitVec, hex::decode},
	value::{
		Value,
		blob::Blob,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, digest::DigestContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::{Decimal, parse::parse_decimal},
		diff_type::DiffType,
		digest::Digest,
		duration::Duration,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		int::{Int, parse::parse_int},
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		row_number::RowNumber,
		system_columns::{SystemColumn, SystemColumns},
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		time::Time,
		uint::{Uint, parse::parse_uint},
		uuid::{
			Uuid4, Uuid7,
			parse::{parse_uuid4, parse_uuid7},
		},
		value_type::ValueType,
	},
};
use serde::de::Error as _;
use serde_json::{Error, Value as JsonValue, from_str, from_value};

use crate::{
	error::DecodeError,
	json::{excerpt, none_marker_depth, types::ResponseFrame},
	tag::peel_options,
};

pub fn frames_from_json(json: &str) -> Result<Vec<Frame>, Error> {
	let response_frames: Vec<ResponseFrame> = from_str(json)?;
	frames_from_response(response_frames)
}

pub fn frames_from_envelope(body: JsonValue) -> Result<Vec<Frame>, Error> {
	let mut envelope = match body {
		JsonValue::Object(envelope) => envelope,
		other => {
			return Err(Error::custom(format!(
				"frames envelope is not an object: {}",
				excerpt(&other.to_string())
			)));
		}
	};
	let frames = envelope.remove("frames").ok_or_else(|| Error::custom("frames envelope has no `frames` key"))?;
	frames_from_response(from_value(frames)?)
}

fn frames_from_response(response_frames: Vec<ResponseFrame>) -> Result<Vec<Frame>, Error> {
	response_frames.into_iter().map(response_frame_to_frame).collect::<Result<_, _>>().map_err(Error::custom)
}

fn response_frame_to_frame(frame: ResponseFrame) -> Result<Frame, DecodeError> {
	let columns = frame
		.columns
		.into_iter()
		.map(|col| {
			Ok(FrameColumn {
				data: convert_column_to_data(&col.name, col.r#type.0, col.payload)?,
				name: col.name,
			})
		})
		.collect::<Result<_, DecodeError>>()?;

	let row_numbers = frame.row_numbers.into_iter().map(RowNumber::new).collect();
	let created_at = system_timestamps(SystemColumn::CreatedAt, &frame.created_at)?;
	let updated_at = system_timestamps(SystemColumn::UpdatedAt, &frame.updated_at)?;
	let time = system_timestamps(SystemColumn::Time, &frame.time)?;
	let op =
		frame.op.map(|raw| {
			DiffType::from_u8(raw)
				.ok_or_else(|| DecodeError::InvalidData(format!("unknown frame op {raw}")))
		})
		.transpose()?;

	Ok(Frame {
		system: SystemColumns::new(row_numbers, Vec::new(), created_at, updated_at, time, Vec::new()),
		columns,
		op,
	})
}

fn system_timestamps(column: SystemColumn, texts: &[String]) -> Result<Vec<DateTime>, DecodeError> {
	texts.iter()
		.enumerate()
		.map(|(row, text)| {
			parse_datetime(Fragment::internal(text))
				.map_err(|_| cell_error(column.name(), row, &ValueType::DateTime, text))
		})
		.collect()
}

fn cell_error(column: &str, row: usize, ty: &ValueType, text: &str) -> DecodeError {
	column_error(column, row, format!("cannot parse '{}' as {ty}", excerpt(text)))
}

fn column_error(column: &str, row: usize, reason: String) -> DecodeError {
	DecodeError::ColumnDecodeFailed {
		column_name: column.to_string(),
		row_index: Some(row),
		source: Box::new(DecodeError::InvalidData(reason)),
	}
}

pub fn parse_value(ty: &ValueType, text: &str) -> Result<Value, DecodeError> {
	let (base, depth) = peel_options(ty);
	match none_marker_depth(text) {
		Some(_) if depth == 0 => Err(DecodeError::InvalidData(format!("none marker for non-Option type {ty}"))),
		Some(wrapped) if wrapped >= depth => Err(DecodeError::InvalidData(format!(
			"none marker depth {wrapped} exceeds the {depth} Option layers of type {ty}"
		))),
		Some(wrapped) => Ok(Value::None {
			inner: strip_options(ty, wrapped + 1),
		}),
		None => parse_base(base, text)
			.ok_or_else(|| DecodeError::InvalidData(format!("cannot parse '{text}' as {ty}"))),
	}
}

pub fn parse_json_value(ty: &ValueType, json: &JsonValue) -> Result<Value, DecodeError> {
	let (base, depth) = peel_options(ty);
	if let Some(wrapped) = json.as_str().and_then(none_marker_depth) {
		return match wrapped {
			_ if depth == 0 => {
				Err(DecodeError::InvalidData(format!("none marker for non-Option type {ty}")))
			}
			wrapped if wrapped >= depth => Err(DecodeError::InvalidData(format!(
				"none marker depth {wrapped} exceeds the {depth} Option layers of type {ty}"
			))),
			wrapped => Ok(Value::None {
				inner: strip_options(ty, wrapped + 1),
			}),
		};
	}
	match base {
		ValueType::List(inner) => {
			let items = json
				.as_array()
				.ok_or_else(|| DecodeError::InvalidData(format!("cannot parse '{json}' as {ty}")))?;
			let values = items
				.iter()
				.map(|item| parse_json_value(inner, item))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Value::List(values))
		}
		ValueType::Record(fields) => {
			let obj = json
				.as_object()
				.ok_or_else(|| DecodeError::InvalidData(format!("cannot parse '{json}' as {ty}")))?;
			let values = fields
				.iter()
				.map(|(name, field_ty)| {
					let item = obj.get(name).ok_or_else(|| {
						DecodeError::InvalidData(format!(
							"record is missing field '{name}' for {ty}"
						))
					})?;
					Ok((name.clone(), parse_json_value(field_ty, item)?))
				})
				.collect::<Result<Vec<_>, DecodeError>>()?;
			Ok(Value::Record(values))
		}
		_ => {
			let text = json.as_str().ok_or_else(|| {
				DecodeError::InvalidData(format!("expected a JSON string for {ty}, got {json}"))
			})?;
			parse_value(ty, text)
		}
	}
}

fn strip_options(ty: &ValueType, layers: u32) -> ValueType {
	let mut inner = ty;
	for _ in 0..layers {
		if let ValueType::Option(next) = inner {
			inner = next;
		}
	}
	inner.clone()
}

fn parse_base(base: &ValueType, text: &str) -> Option<Value> {
	match base {
		ValueType::Boolean => text.parse().ok().map(Value::Boolean),
		ValueType::Float4 => parse_float4(text).map(Value::Float4),
		ValueType::Float8 => parse_float8(text).map(Value::Float8),
		ValueType::Int1 => text.parse().ok().map(Value::Int1),
		ValueType::Int2 => text.parse().ok().map(Value::Int2),
		ValueType::Int4 => text.parse().ok().map(Value::Int4),
		ValueType::Int8 => text.parse().ok().map(Value::Int8),
		ValueType::Int16 => text.parse().ok().map(Value::Int16),
		ValueType::Utf8 => Some(Value::Utf8(text.to_string())),
		ValueType::Uint1 => text.parse().ok().map(Value::Uint1),
		ValueType::Uint2 => text.parse().ok().map(Value::Uint2),
		ValueType::Uint4 => text.parse().ok().map(Value::Uint4),
		ValueType::Uint8 => text.parse().ok().map(Value::Uint8),
		ValueType::Uint16 => text.parse().ok().map(Value::Uint16),
		ValueType::Date => parse_date_text(text).map(Value::Date),
		ValueType::DateTime => parse_datetime_text(text).map(Value::DateTime),
		ValueType::Time => parse_time_text(text).map(Value::Time),
		ValueType::Duration => parse_duration_text(text).map(Value::Duration),
		ValueType::Uuid4 => parse_uuid4_text(text).map(Value::Uuid4),
		ValueType::Uuid7 => parse_uuid7_text(text).map(Value::Uuid7),
		ValueType::IdentityId => parse_identity_id(text).map(Value::IdentityId),
		ValueType::Blob => parse_blob(text).map(Value::Blob),
		ValueType::Int => parse_int_text(text).map(Value::Int),
		ValueType::Uint => parse_uint_text(text).map(Value::Uint),
		ValueType::Decimal => parse_decimal_text(text).map(Value::Decimal),
		ValueType::Option(inner) => parse_base(inner, text),
		ValueType::Digest {
			inner,
			accuracy,
		} => parse_digest_text(inner, *accuracy, text).map(|digest| Value::Digest(Box::new(digest))),
		ValueType::Any
		| ValueType::DictionaryId
		| ValueType::List(_)
		| ValueType::Record(_)
		| ValueType::Tuple(_) => Some(Value::Utf8(text.to_string())),
	}
}

fn parse_float4(text: &str) -> Option<OrderedF32> {
	OrderedF32::try_from(text.parse::<f32>().ok()?).ok()
}

fn parse_float8(text: &str) -> Option<OrderedF64> {
	OrderedF64::try_from(text.parse::<f64>().ok()?).ok()
}

fn parse_date_text(text: &str) -> Option<Date> {
	parse_date(Fragment::internal(text)).ok()
}

fn parse_datetime_text(text: &str) -> Option<DateTime> {
	parse_datetime(Fragment::internal(text))
		.ok()
		.or_else(|| DateTime::from_epoch_secs(text.parse::<i64>().ok()?).ok())
}

fn parse_time_text(text: &str) -> Option<Time> {
	parse_time(Fragment::internal(text)).ok()
}

fn parse_duration_text(text: &str) -> Option<Duration> {
	parse_duration(Fragment::internal(text)).ok()
}

fn parse_uuid4_text(text: &str) -> Option<Uuid4> {
	parse_uuid4(Fragment::internal(text)).ok()
}

fn parse_uuid7_text(text: &str) -> Option<Uuid7> {
	parse_uuid7(Fragment::internal(text)).ok()
}

fn parse_identity_id(text: &str) -> Option<IdentityId> {
	parse_uuid7_text(text).map(IdentityId::from)
}

fn parse_blob(text: &str) -> Option<Blob> {
	Blob::from_hex(Fragment::internal(text)).ok()
}

fn parse_digest_text(inner: &ValueType, accuracy: u32, text: &str) -> Option<Digest> {
	let bytes = decode(text.strip_prefix("0x")?).ok()?;
	let digest = Digest::decode(&bytes).ok()?;
	(*digest.inner() == *inner && digest.accuracy() == accuracy).then_some(digest)
}

fn parse_int_text(text: &str) -> Option<Int> {
	parse_int(Fragment::internal(text)).ok()
}

fn parse_uint_text(text: &str) -> Option<Uint> {
	parse_uint(Fragment::internal(text)).ok()
}

fn parse_decimal_text(text: &str) -> Option<Decimal> {
	parse_decimal(Fragment::internal(text)).ok()
}

fn cells<T: Clone>(
	name: &str,
	ty: &ValueType,
	rows: Vec<Option<String>>,
	filler: T,
	parse: impl Fn(&str) -> Option<T>,
) -> Result<Vec<T>, DecodeError> {
	rows.into_iter()
		.enumerate()
		.map(|(row, cell)| match cell {
			None => Ok(filler.clone()),
			Some(text) => parse(&text).ok_or_else(|| cell_error(name, row, ty, &text)),
		})
		.collect()
}

pub fn convert_column_to_data(
	name: &str,
	target: ValueType,
	data: Vec<JsonValue>,
) -> Result<FrameColumnData, DecodeError> {
	let (base, depth) = peel_options(&target);
	if matches!(base, ValueType::List(_) | ValueType::Record(_)) {
		return convert_list_or_record_column(name, &target, base, depth, data);
	}
	let data: Vec<String> = data
		.into_iter()
		.enumerate()
		.map(|(row, payload)| match payload {
			JsonValue::String(text) => Ok(text),
			other => Err(column_error(
				name,
				row,
				format!("expected a JSON string for {target}, got {other}"),
			)),
		})
		.collect::<Result<_, _>>()?;
	let mut layers = vec![vec![true; data.len()]; depth as usize];
	let mut rows = Vec::with_capacity(data.len());
	for (row, payload) in data.into_iter().enumerate() {
		match none_marker_depth(&payload) {
			None => rows.push(Some(payload)),
			Some(wrapped) if wrapped < depth => {
				for layer in &mut layers[wrapped as usize..] {
					layer[row] = false;
				}
				rows.push(None);
			}
			Some(0) if matches!(base, ValueType::Digest { .. }) => rows.push(None),
			Some(_) if depth == 0 => {
				return Err(column_error(
					name,
					row,
					format!("none marker for non-Option type {target}"),
				));
			}
			Some(wrapped) => {
				return Err(column_error(
					name,
					row,
					format!(
						"none marker depth {wrapped} exceeds the {depth} Option layers of type {target}"
					),
				));
			}
		}
	}
	let base = base_column(name, base, rows)?;
	Ok(layers.into_iter().rev().fold(base, |inner, layer| FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BitVec::from_slice(&layer),
	}))
}

fn convert_list_or_record_column(
	name: &str,
	target: &ValueType,
	base: &ValueType,
	depth: u32,
	data: Vec<JsonValue>,
) -> Result<FrameColumnData, DecodeError> {
	let mut layers = vec![vec![true; data.len()]; depth as usize];
	let mut values = Vec::with_capacity(data.len());
	for (row, payload) in data.into_iter().enumerate() {
		match payload.as_str().and_then(none_marker_depth) {
			None => {
				let value = parse_json_value(base, &payload)
					.map_err(|e| column_error(name, row, e.to_string()))?;
				values.push(value);
			}
			Some(wrapped) if wrapped < depth => {
				for layer in &mut layers[wrapped as usize..] {
					layer[row] = false;
				}
				values.push(Value::none());
			}
			Some(_) if depth == 0 => {
				return Err(column_error(
					name,
					row,
					format!("none marker for non-Option type {target}"),
				));
			}
			Some(wrapped) => {
				return Err(column_error(
					name,
					row,
					format!(
						"none marker depth {wrapped} exceeds the {depth} Option layers of type {target}"
					),
				));
			}
		}
	}
	let base_col = FrameColumnData::Any(AnyContainer::from_vec(values).with_declared_type(base.clone()));
	Ok(layers.into_iter().rev().fold(base_col, |inner, layer| FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BitVec::from_slice(&layer),
	}))
}

fn base_column(name: &str, base: &ValueType, rows: Vec<Option<String>>) -> Result<FrameColumnData, DecodeError> {
	Ok(match base {
		ValueType::Option(inner) => base_column(name, inner, rows)?,
		ValueType::Boolean => {
			FrameColumnData::Bool(BoolContainer::new(cells(name, base, rows, false, |s| s.parse().ok())?))
		}
		ValueType::Float4 => {
			FrameColumnData::Float4(NumberContainer::new(cells(name, base, rows, 0.0f32, |s| {
				s.parse().ok()
			})?))
		}
		ValueType::Float8 => {
			FrameColumnData::Float8(NumberContainer::new(cells(name, base, rows, 0.0f64, |s| {
				s.parse().ok()
			})?))
		}
		ValueType::Int1 => {
			FrameColumnData::Int1(NumberContainer::new(cells(name, base, rows, 0i8, |s| s.parse().ok())?))
		}
		ValueType::Int2 => {
			FrameColumnData::Int2(NumberContainer::new(cells(name, base, rows, 0i16, |s| s.parse().ok())?))
		}
		ValueType::Int4 => {
			FrameColumnData::Int4(NumberContainer::new(cells(name, base, rows, 0i32, |s| s.parse().ok())?))
		}
		ValueType::Int8 => {
			FrameColumnData::Int8(NumberContainer::new(cells(name, base, rows, 0i64, |s| s.parse().ok())?))
		}
		ValueType::Int16 => {
			FrameColumnData::Int16(NumberContainer::new(cells(name, base, rows, 0i128, |s| {
				s.parse().ok()
			})?))
		}
		ValueType::Uint1 => {
			FrameColumnData::Uint1(NumberContainer::new(cells(name, base, rows, 0u8, |s| s.parse().ok())?))
		}
		ValueType::Uint2 => {
			FrameColumnData::Uint2(NumberContainer::new(cells(name, base, rows, 0u16, |s| s.parse().ok())?))
		}
		ValueType::Uint4 => {
			FrameColumnData::Uint4(NumberContainer::new(cells(name, base, rows, 0u32, |s| s.parse().ok())?))
		}
		ValueType::Uint8 => {
			FrameColumnData::Uint8(NumberContainer::new(cells(name, base, rows, 0u64, |s| s.parse().ok())?))
		}
		ValueType::Uint16 => {
			FrameColumnData::Uint16(NumberContainer::new(cells(name, base, rows, 0u128, |s| {
				s.parse().ok()
			})?))
		}
		ValueType::Date => FrameColumnData::Date(TemporalContainer::new(cells(
			name,
			base,
			rows,
			Date::from_ymd(1970, 1, 1).unwrap(),
			parse_date_text,
		)?)),
		ValueType::DateTime => FrameColumnData::DateTime(TemporalContainer::new(cells(
			name,
			base,
			rows,
			DateTime::from_epoch_secs(0).unwrap(),
			parse_datetime_text,
		)?)),
		ValueType::Time => FrameColumnData::Time(TemporalContainer::new(cells(
			name,
			base,
			rows,
			Time::from_hms(0, 0, 0).unwrap(),
			parse_time_text,
		)?)),
		ValueType::Duration => FrameColumnData::Duration(TemporalContainer::new(cells(
			name,
			base,
			rows,
			Duration::zero(),
			parse_duration_text,
		)?)),
		ValueType::Uuid4 => FrameColumnData::Uuid4(UuidContainer::new(cells(
			name,
			base,
			rows,
			parse_uuid4_text("00000000-0000-4000-8000-000000000000").unwrap(),
			parse_uuid4_text,
		)?)),
		ValueType::Uuid7 => FrameColumnData::Uuid7(UuidContainer::new(cells(
			name,
			base,
			rows,
			parse_uuid7_text("00000000-0000-7000-8000-000000000000").unwrap(),
			parse_uuid7_text,
		)?)),
		ValueType::IdentityId => FrameColumnData::IdentityId(IdentityIdContainer::new(cells(
			name,
			base,
			rows,
			parse_identity_id("00000000-0000-7000-8000-000000000000").unwrap(),
			parse_identity_id,
		)?)),
		ValueType::Blob => FrameColumnData::Blob(BlobContainer::new(cells(
			name,
			base,
			rows,
			Blob::new(vec![]),
			parse_blob,
		)?)),
		ValueType::Int => FrameColumnData::Int(NumberContainer::new(cells(
			name,
			base,
			rows,
			Int::zero(),
			parse_int_text,
		)?)),
		ValueType::Uint => FrameColumnData::Uint(NumberContainer::new(cells(
			name,
			base,
			rows,
			Uint::zero(),
			parse_uint_text,
		)?)),
		ValueType::Decimal => FrameColumnData::Decimal(NumberContainer::new(cells(
			name,
			base,
			rows,
			Decimal::zero(),
			parse_decimal_text,
		)?)),
		ValueType::Digest {
			inner,
			accuracy,
		} => {
			let mut container = DigestContainer::with_capacity(rows.len());
			for (row, cell) in rows.into_iter().enumerate() {
				match cell {
					None => container.push_default(),
					Some(text) => {
						let digest = parse_digest_text(inner, *accuracy, &text)
							.ok_or_else(|| cell_error(name, row, base, &text))?;
						container.push(Box::new(digest));
					}
				}
			}
			FrameColumnData::Digest {
				container,
				inner: inner.as_ref().clone(),
				accuracy: *accuracy,
			}
		}
		ValueType::Utf8
		| ValueType::Any
		| ValueType::DictionaryId
		| ValueType::List(_)
		| ValueType::Record(_)
		| ValueType::Tuple(_) => FrameColumnData::Utf8(Utf8Container::new(cells(name, base, rows, String::new(), |s| {
			Some(s.to_string())
		})?)),
	})
}
