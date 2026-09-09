// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		Value,
		blob::Blob,
		container::{
			blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::{Decimal, parse::parse_decimal},
		diff_type::DiffType,
		duration::Duration,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		int::{Int, parse::parse_int},
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		row_number::RowNumber,
		system_columns::SystemColumns,
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
use serde_json::{Error, Value as JsonValue, from_str, from_value};

use crate::{
	error::DecodeError,
	json::{is_none_marker, none_marker_depth, types::ResponseFrame},
	tag::peel_options,
};

pub fn frames_from_json(json: &str) -> Result<Vec<Frame>, Error> {
	let response_frames: Vec<ResponseFrame> = from_str(json)?;
	Ok(response_frames.into_iter().map(response_frame_to_frame).collect())
}

pub fn convert_envelope_response(body: JsonValue) -> Vec<Frame> {
	let frames_value = match body {
		JsonValue::Object(ref map) => map.get("frames"),
		_ => None,
	};

	let response_frames: Vec<ResponseFrame> = match frames_value {
		Some(v) => from_value(v.clone()).unwrap_or_default(),
		None => return Vec::new(),
	};

	response_frames.into_iter().map(response_frame_to_frame).collect()
}

fn response_frame_to_frame(frame: ResponseFrame) -> Frame {
	let columns = frame
		.columns
		.into_iter()
		.map(|col| FrameColumn {
			name: col.name,
			data: convert_column_to_data(col.r#type.0, col.payload),
		})
		.collect();

	let row_numbers = frame.row_numbers.into_iter().map(RowNumber::new).collect();
	let created_at = frame.created_at.iter().filter_map(|s| parse_datetime(Fragment::internal(s)).ok()).collect();
	let updated_at = frame.updated_at.iter().filter_map(|s| parse_datetime(Fragment::internal(s)).ok()).collect();
	let time = frame.time.iter().filter_map(|s| parse_datetime(Fragment::internal(s)).ok()).collect();

	Frame {
		system: SystemColumns::new(row_numbers, Vec::new(), created_at, updated_at, time),
		columns,
		op: frame.op.and_then(DiffType::from_u8),
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

fn parse_int_text(text: &str) -> Option<Int> {
	parse_int(Fragment::internal(text)).ok()
}

fn parse_uint_text(text: &str) -> Option<Uint> {
	parse_uint(Fragment::internal(text)).ok()
}

fn parse_decimal_text(text: &str) -> Option<Decimal> {
	parse_decimal(Fragment::internal(text)).ok()
}

fn cells<T: Clone>(data: Vec<String>, placeholder: T, parse: impl Fn(&str) -> Option<T>) -> Vec<T> {
	data.into_iter()
		.map(|s| {
			if is_none_marker(&s) {
				placeholder.clone()
			} else {
				parse(&s).unwrap_or_else(|| placeholder.clone())
			}
		})
		.collect()
}

pub fn convert_column_to_data(target: ValueType, data: Vec<String>) -> FrameColumnData {
	match target {
		ValueType::Option(_) => {
			let (base, depth) = peel_options(&target);
			let depth = depth as usize;
			let mut layers = vec![vec![true; data.len()]; depth];
			for (row, payload) in data.iter().enumerate() {
				if let Some(wrapped) = none_marker_depth(payload) {
					let first_undefined = (wrapped as usize).min(depth - 1);
					for layer in &mut layers[first_undefined..] {
						layer[row] = false;
					}
				}
			}
			let base = convert_column_to_data(base.clone(), data);
			layers.into_iter().rev().fold(base, |inner, layer| FrameColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from_slice(&layer),
			})
		}
		ValueType::Boolean => FrameColumnData::Bool(BoolContainer::new(cells(data, false, |s| s.parse().ok()))),
		ValueType::Float4 => {
			FrameColumnData::Float4(NumberContainer::new(cells(data, 0.0f32, |s| s.parse().ok())))
		}
		ValueType::Float8 => {
			FrameColumnData::Float8(NumberContainer::new(cells(data, 0.0f64, |s| s.parse().ok())))
		}
		ValueType::Int1 => FrameColumnData::Int1(NumberContainer::new(cells(data, 0i8, |s| s.parse().ok()))),
		ValueType::Int2 => FrameColumnData::Int2(NumberContainer::new(cells(data, 0i16, |s| s.parse().ok()))),
		ValueType::Int4 => FrameColumnData::Int4(NumberContainer::new(cells(data, 0i32, |s| s.parse().ok()))),
		ValueType::Int8 => FrameColumnData::Int8(NumberContainer::new(cells(data, 0i64, |s| s.parse().ok()))),
		ValueType::Int16 => {
			FrameColumnData::Int16(NumberContainer::new(cells(data, 0i128, |s| s.parse().ok())))
		}
		ValueType::Uint1 => FrameColumnData::Uint1(NumberContainer::new(cells(data, 0u8, |s| s.parse().ok()))),
		ValueType::Uint2 => FrameColumnData::Uint2(NumberContainer::new(cells(data, 0u16, |s| s.parse().ok()))),
		ValueType::Uint4 => FrameColumnData::Uint4(NumberContainer::new(cells(data, 0u32, |s| s.parse().ok()))),
		ValueType::Uint8 => FrameColumnData::Uint8(NumberContainer::new(cells(data, 0u64, |s| s.parse().ok()))),
		ValueType::Uint16 => {
			FrameColumnData::Uint16(NumberContainer::new(cells(data, 0u128, |s| s.parse().ok())))
		}
		ValueType::Date => FrameColumnData::Date(TemporalContainer::new(cells(
			data,
			Date::from_ymd(1970, 1, 1).unwrap(),
			parse_date_text,
		))),
		ValueType::DateTime => FrameColumnData::DateTime(TemporalContainer::new(cells(
			data,
			DateTime::from_epoch_secs(0).unwrap(),
			parse_datetime_text,
		))),
		ValueType::Time => FrameColumnData::Time(TemporalContainer::new(cells(
			data,
			Time::from_hms(0, 0, 0).unwrap(),
			parse_time_text,
		))),
		ValueType::Duration => FrameColumnData::Duration(TemporalContainer::new(cells(
			data,
			Duration::zero(),
			parse_duration_text,
		))),
		ValueType::Uuid4 => FrameColumnData::Uuid4(UuidContainer::new(cells(
			data,
			parse_uuid4_text("00000000-0000-4000-8000-000000000000").unwrap(),
			parse_uuid4_text,
		))),
		ValueType::Uuid7 => FrameColumnData::Uuid7(UuidContainer::new(cells(
			data,
			parse_uuid7_text("00000000-0000-7000-8000-000000000000").unwrap(),
			parse_uuid7_text,
		))),
		ValueType::IdentityId => FrameColumnData::IdentityId(IdentityIdContainer::new(cells(
			data,
			parse_identity_id("00000000-0000-7000-8000-000000000000").unwrap(),
			parse_identity_id,
		))),
		ValueType::Blob => {
			FrameColumnData::Blob(BlobContainer::new(cells(data, Blob::new(vec![]), parse_blob)))
		}
		ValueType::Int => FrameColumnData::Int(NumberContainer::new(cells(data, Int::zero(), parse_int_text))),
		ValueType::Uint => {
			FrameColumnData::Uint(NumberContainer::new(cells(data, Uint::zero(), parse_uint_text)))
		}
		ValueType::Decimal => {
			FrameColumnData::Decimal(NumberContainer::new(cells(data, Decimal::zero(), parse_decimal_text)))
		}
		ValueType::Utf8
		| ValueType::Any
		| ValueType::DictionaryId
		| ValueType::List(_)
		| ValueType::Record(_)
		| ValueType::Tuple(_) => {
			FrameColumnData::Utf8(Utf8Container::new(cells(data, String::new(), |s| Some(s.to_string()))))
		}
	}
}
