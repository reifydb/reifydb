// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	fragment::Fragment,
	util::{bitvec::BitVec, hex},
	value::{
		blob::Blob,
		container::{
			blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
			vector::VectorContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		int::{Int, parse::parse_int},
		row_number::RowNumber,
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		time::Time,
		uint::{Uint, parse::parse_uint},
		uuid::parse::{parse_uuid4, parse_uuid7},
		value_type::ValueType,
	},
};
use serde_json::{Error, Value, from_str, from_value};

use crate::json::types::ResponseFrame;

pub fn frames_from_json(json: &str) -> Result<Vec<Frame>, Error> {
	let response_frames: Vec<ResponseFrame> = from_str(json)?;
	Ok(response_frames.into_iter().map(response_frame_to_frame).collect())
}

pub fn convert_envelope_response(body: Value) -> Vec<Frame> {
	let frames_value = match body {
		Value::Object(ref map) => map.get("frames"),
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
			data: convert_column_to_data(col.r#type, col.payload),
		})
		.collect();

	let row_numbers = frame.row_numbers.into_iter().map(RowNumber::new).collect();
	let created_at = frame.created_at.iter().filter_map(|s| parse_datetime(Fragment::internal(s)).ok()).collect();
	let updated_at = frame.updated_at.iter().filter_map(|s| parse_datetime(Fragment::internal(s)).ok()).collect();

	Frame {
		row_numbers,
		created_at,
		updated_at,
		columns,
	}
}

fn parse_vector_literal(s: &str) -> Option<Vec<f32>> {
	let inner = s.trim().strip_prefix('[')?.strip_suffix(']')?;
	if inner.trim().is_empty() {
		return Some(Vec::new());
	}
	inner.split(',').map(|part| part.trim().parse::<f32>().ok()).collect()
}

pub fn convert_column_to_data(target: ValueType, data: Vec<String>) -> FrameColumnData {
	match target {
		ValueType::Option(inner_type) => {
			let defined: Vec<bool> = data.iter().map(|s| s != "⟪none⟫").collect();

			if defined.iter().all(|&b| !b) {
				let inner = convert_column_to_data(*inner_type, data);
				return FrameColumnData::Option {
					inner: Box::new(inner),
					bitvec: BitVec::from_slice(&defined),
				};
			}

			let bitvec = BitVec::from_slice(&defined);
			let inner = convert_column_to_data(*inner_type, data);

			if defined.iter().all(|&b| b) {
				return inner;
			}

			FrameColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
		ValueType::Boolean => {
			let values: Vec<_> = data.into_iter().map(|s| s != "⟪none⟫" && s == "true").collect();
			FrameColumnData::Bool(BoolContainer::new(values))
		}
		ValueType::Float4 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0.0f32
					} else {
						s.parse::<f32>().unwrap_or(0.0)
					}
				})
				.collect();
			FrameColumnData::Float4(NumberContainer::new(values))
		}
		ValueType::Float8 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0.0f64
					} else {
						s.parse::<f64>().unwrap_or(0.0)
					}
				})
				.collect();
			FrameColumnData::Float8(NumberContainer::new(values))
		}
		ValueType::Int1 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0i8
					} else {
						s.parse::<i8>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Int1(NumberContainer::new(values))
		}
		ValueType::Int2 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0i16
					} else {
						s.parse::<i16>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Int2(NumberContainer::new(values))
		}
		ValueType::Int4 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0i32
					} else {
						s.parse::<i32>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Int4(NumberContainer::new(values))
		}
		ValueType::Int8 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0i64
					} else {
						s.parse::<i64>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Int8(NumberContainer::new(values))
		}
		ValueType::Int16 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0i128
					} else {
						s.parse::<i128>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Int16(NumberContainer::new(values))
		}
		ValueType::Uint1 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0u8
					} else {
						s.parse::<u8>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Uint1(NumberContainer::new(values))
		}
		ValueType::Uint2 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0u16
					} else {
						s.parse::<u16>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Uint2(NumberContainer::new(values))
		}
		ValueType::Uint4 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0u32
					} else {
						s.parse::<u32>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Uint4(NumberContainer::new(values))
		}
		ValueType::Uint8 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0u64
					} else {
						s.parse::<u64>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Uint8(NumberContainer::new(values))
		}
		ValueType::Uint16 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						0u128
					} else {
						s.parse::<u128>().unwrap_or(0)
					}
				})
				.collect();
			FrameColumnData::Uint16(NumberContainer::new(values))
		}
		ValueType::Utf8 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						String::new()
					} else {
						s
					}
				})
				.collect();
			FrameColumnData::Utf8(Utf8Container::new(values))
		}
		ValueType::Date => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Date::from_ymd(1970, 1, 1).unwrap()
					} else {
						parse_date(Fragment::from(s))
							.unwrap_or_else(|_| Date::from_ymd(1970, 1, 1).unwrap())
					}
				})
				.collect();
			FrameColumnData::Date(TemporalContainer::new(values))
		}
		ValueType::DateTime => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						DateTime::from_timestamp(0).unwrap()
					} else if let Ok(dt) = parse_datetime(Fragment::testing(&s)) {
						dt
					} else if let Ok(timestamp) = s.parse::<i64>() {
						DateTime::from_timestamp(timestamp)
							.unwrap_or(DateTime::from_timestamp(0).unwrap())
					} else {
						DateTime::from_timestamp(0).unwrap()
					}
				})
				.collect();
			FrameColumnData::DateTime(TemporalContainer::new(values))
		}
		ValueType::Time => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Time::from_hms(0, 0, 0).unwrap()
					} else {
						parse_time(Fragment::from(s))
							.unwrap_or_else(|_| Time::from_hms(0, 0, 0).unwrap())
					}
				})
				.collect();
			FrameColumnData::Time(TemporalContainer::new(values))
		}
		ValueType::Duration => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Duration::zero()
					} else {
						parse_duration(Fragment::from(s)).unwrap_or_else(|_| Duration::zero())
					}
				})
				.collect();
			FrameColumnData::Duration(TemporalContainer::new(values))
		}
		ValueType::Uuid4 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						parse_uuid4("00000000-0000-4000-8000-000000000000".into()).unwrap()
					} else if let Ok(uuid) = parse_uuid4(s.into()) {
						uuid
					} else {
						parse_uuid4("00000000-0000-4000-8000-000000000000".into()).unwrap()
					}
				})
				.collect();
			FrameColumnData::Uuid4(UuidContainer::new(values))
		}
		ValueType::Uuid7 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						parse_uuid7("00000000-0000-7000-8000-000000000000".into()).unwrap()
					} else if let Ok(uuid) = parse_uuid7(s.into()) {
						uuid
					} else {
						parse_uuid7("00000000-0000-7000-8000-000000000000".into()).unwrap()
					}
				})
				.collect();
			FrameColumnData::Uuid7(UuidContainer::new(values))
		}
		ValueType::IdentityId => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						IdentityId::from(
							parse_uuid7("00000000-0000-7000-8000-000000000000".into())
								.unwrap(),
						)
					} else if let Ok(uuid) = parse_uuid7(s.into()) {
						IdentityId::from(uuid)
					} else {
						IdentityId::from(
							parse_uuid7("00000000-0000-7000-8000-000000000000".into())
								.unwrap(),
						)
					}
				})
				.collect();
			FrameColumnData::IdentityId(IdentityIdContainer::new(values))
		}
		ValueType::Blob => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Blob::new(vec![])
					} else if let Some(hex_str) = s.strip_prefix("0x") {
						if let Ok(bytes) = hex::decode(hex_str) {
							Blob::new(bytes)
						} else {
							Blob::new(vec![])
						}
					} else {
						Blob::new(vec![])
					}
				})
				.collect();
			FrameColumnData::Blob(BlobContainer::new(values))
		}
		ValueType::Vector(_) => {
			let parsed: Vec<Option<Vec<f32>>> = data
				.iter()
				.map(|s| {
					if s == "⟪none⟫" {
						None
					} else {
						parse_vector_literal(s)
					}
				})
				.collect();

			let dims = parsed.iter().flatten().map(|v| v.len()).find(|len| *len > 0).unwrap_or(1);
			let filler = vec![0.0f32; dims];

			let mut container = VectorContainer::with_capacity(dims as u32, parsed.len());
			for entry in &parsed {
				match entry {
					Some(values) if values.len() == dims => container.push(values),
					_ => container.push(&filler),
				}
			}
			FrameColumnData::Vector(container)
		}
		ValueType::Int => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Int::zero()
					} else {
						parse_int(Fragment::from(s)).unwrap_or_else(|_| Int::zero())
					}
				})
				.collect();
			FrameColumnData::Int(NumberContainer::new(values))
		}
		ValueType::Uint => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Uint::zero()
					} else {
						parse_uint(Fragment::from(s)).unwrap_or_else(|_| Uint::zero())
					}
				})
				.collect();
			FrameColumnData::Uint(NumberContainer::new(values))
		}
		ValueType::Decimal => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Decimal::zero()
					} else {
						s.parse::<Decimal>().unwrap_or_else(|_| Decimal::zero())
					}
				})
				.collect();
			FrameColumnData::Decimal(NumberContainer::new(values))
		}
		ValueType::Any
		| ValueType::DictionaryId
		| ValueType::List(_)
		| ValueType::Record(_)
		| ValueType::Tuple(_) => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						String::new()
					} else {
						s
					}
				})
				.collect();
			FrameColumnData::Utf8(Utf8Container::new(values))
		}
	}
}
