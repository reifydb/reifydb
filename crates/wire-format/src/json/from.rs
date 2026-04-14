// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	util::{bitvec::BitVec, hex},
	value::{
		blob::Blob,
		container::{
			blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		int::{Int, parse::parse_int},
		row_number::RowNumber,
		temporal::parse::{datetime::parse_datetime, duration::parse_duration},
		time::Time,
		r#type::Type,
		uint::{Uint, parse::parse_uint},
		uuid::parse::{parse_uuid4, parse_uuid7},
	},
};
use serde_json::{Error, Value, from_str, from_value};

use crate::json::types::ResponseFrame;

/// Parse a JSON string in the `[ResponseFrame, ...]` shape and rebuild typed `Frame`s.
pub fn frames_from_json(json: &str) -> Result<Vec<Frame>, Error> {
	let response_frames: Vec<ResponseFrame> = from_str(json)?;
	Ok(response_frames.into_iter().map(response_frame_to_frame).collect())
}

/// Convert a top-level envelope body containing `{ "frames": [...] }` to `Vec<Frame>`.
///
/// Used by JSON transport clients that embed frames inside a response envelope.
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

/// Parse a column's payload strings back into typed `FrameColumnData`.
pub fn convert_column_to_data(target: Type, data: Vec<String>) -> FrameColumnData {
	match target {
		Type::Option(inner_type) => {
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
		Type::Boolean => {
			let values: Vec<_> = data.into_iter().map(|s| s != "⟪none⟫" && s == "true").collect();
			FrameColumnData::Bool(BoolContainer::new(values))
		}
		Type::Float4 => {
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
		Type::Float8 => {
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
		Type::Int1 => {
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
		Type::Int2 => {
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
		Type::Int4 => {
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
		Type::Int8 => {
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
		Type::Int16 => {
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
		Type::Uint1 => {
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
		Type::Uint2 => {
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
		Type::Uint4 => {
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
		Type::Uint8 => {
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
		Type::Uint16 => {
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
		Type::Utf8 => {
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
		Type::Date => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Date::from_ymd(1970, 1, 1).unwrap()
					} else {
						let parts: Vec<&str> = s.split('-').collect();
						if parts.len() == 3 {
							let year = parts[0].parse::<i32>().unwrap_or(1970);
							let month = parts[1].parse::<u32>().unwrap_or(1);
							let day = parts[2].parse::<u32>().unwrap_or(1);
							Date::from_ymd(year, month, day)
								.unwrap_or(Date::from_ymd(1970, 1, 1).unwrap())
						} else {
							Date::from_ymd(1970, 1, 1).unwrap()
						}
					}
				})
				.collect();
			FrameColumnData::Date(TemporalContainer::new(values))
		}
		Type::DateTime => {
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
		Type::Time => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						Time::from_hms(0, 0, 0).unwrap()
					} else {
						let parts: Vec<&str> = s.split(':').collect();
						if parts.len() >= 3 {
							let hour = parts[0].parse::<u32>().unwrap_or(0);
							let min = parts[1].parse::<u32>().unwrap_or(0);
							let sec_parts: Vec<&str> = parts[2].split('.').collect();
							let sec = sec_parts[0].parse::<u32>().unwrap_or(0);
							let nano = if sec_parts.len() > 1 {
								let frac_str = sec_parts[1];
								let padded = if frac_str.len() < 9 {
									format!("{:0<9}", frac_str)
								} else {
									frac_str[..9].to_string()
								};
								padded.parse::<u32>().unwrap_or(0)
							} else {
								0
							};
							Time::from_hms_nano(hour, min, sec, nano)
								.unwrap_or(Time::from_hms(0, 0, 0).unwrap())
						} else {
							Time::from_hms(0, 0, 0).unwrap()
						}
					}
				})
				.collect();
			FrameColumnData::Time(TemporalContainer::new(values))
		}
		Type::Duration => {
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
		Type::Uuid4 => {
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
		Type::Uuid7 => {
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
		Type::IdentityId => {
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
		Type::Blob => {
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
		Type::Int => {
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
		Type::Uint => {
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
		Type::Decimal => {
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
		Type::Any
		| Type::DictionaryId
		| Type::List(_)
		| Type::Record(_)
		| Type::Tuple(_) => {
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
