// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use reifydb_type::{
	BitVec, Blob, Date, DateTime, Error, Frame, FrameColumn, FrameColumnData, IdentityId, OwnedFragment, RowNumber,
	Time, Uuid7, err, parse_datetime, parse_uuid4, parse_uuid7,
	util::hex,
	value::container::{
		BlobContainer, BoolContainer, IdentityIdContainer, NumberContainer, TemporalContainer,
		UndefinedContainer, Utf8Container, UuidContainer,
	},
};

use crate::Type;

/// Result type for command operations
#[derive(Debug)]
pub struct CommandResult {
	pub frames: Vec<Frame>,
}

/// Result type for query operations
#[derive(Debug)]
pub struct QueryResult {
	pub frames: Vec<Frame>,
}

// Helper functions for parsing responses - made public for ws module
pub fn parse_command_response(response: crate::Response) -> Result<CommandResult, Error> {
	match response.payload {
		crate::ResponsePayload::Command(cmd_response) => Ok(CommandResult {
			frames: convert_command_response(cmd_response),
		}),
		crate::ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => {
			println!("Unexpected execute response: {:?}", other);
			panic!("Unexpected execute response type")
		}
	}
}

pub fn parse_query_response(response: crate::Response) -> Result<QueryResult, Error> {
	match response.payload {
		crate::ResponsePayload::Query(query_response) => {
			let frames = convert_query_response(query_response);
			Ok(QueryResult {
				frames,
			})
		}
		crate::ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => {
			println!("Unexpected execute response: {:?}", other);
			panic!("Unexpected execute response type")
		}
	}
}

pub fn convert_command_response(payload: crate::CommandResponse) -> Vec<Frame> {
	let mut result = Vec::new();

	for frame in payload.frames {
		let columns = frame
			.columns
			.into_iter()
			.map(|col| FrameColumn {
				namespace: col.namespace,
				source: col.store,
				name: col.name,
				data: convert_column_to_data(col.r#type, col.data),
			})
			.collect();

		let row_numbers = frame.row_numbers.into_iter().map(RowNumber::new).collect();
		result.push(Frame::with_row_numbers(columns, row_numbers))
	}

	result
}

pub fn convert_query_response(payload: crate::QueryResponse) -> Vec<Frame> {
	let mut result = Vec::new();

	for frame in payload.frames {
		let columns = frame
			.columns
			.into_iter()
			.map(|col| FrameColumn {
				namespace: col.namespace,
				source: col.store,
				name: col.name,
				data: convert_column_to_data(col.r#type, col.data),
			})
			.collect();

		let row_numbers = frame.row_numbers.into_iter().map(RowNumber::new).collect();
		result.push(Frame::with_row_numbers(columns, row_numbers))
	}

	result
}

fn convert_column_to_data(target: Type, data: Vec<String>) -> FrameColumnData {
	let len = data.len();

	match target {
		Type::Undefined => FrameColumnData::Undefined(UndefinedContainer::new(len)),
		Type::Boolean => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(false, false)
					} else {
						(s == "true", true)
					}
				})
				.unzip();
			FrameColumnData::Bool(BoolContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Float4 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0.0f32, false)
					} else {
						(s.parse::<f32>().unwrap_or(0.0), true)
					}
				})
				.unzip();
			FrameColumnData::Float4(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Float8 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0.0f64, false)
					} else {
						(s.parse::<f64>().unwrap_or(0.0), true)
					}
				})
				.unzip();
			FrameColumnData::Float8(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Int1 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0i8, false)
					} else {
						(s.parse::<i8>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Int1(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Int2 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0i16, false)
					} else {
						(s.parse::<i16>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Int2(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Int4 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0i32, false)
					} else {
						(s.parse::<i32>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Int4(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Int8 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0i64, false)
					} else {
						(s.parse::<i64>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Int8(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Int16 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0i128, false)
					} else {
						(s.parse::<i128>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Int16(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uint1 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0u8, false)
					} else {
						(s.parse::<u8>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Uint1(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uint2 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0u16, false)
					} else {
						(s.parse::<u16>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Uint2(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uint4 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0u32, false)
					} else {
						(s.parse::<u32>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Uint4(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uint8 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0u64, false)
					} else {
						(s.parse::<u64>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Uint8(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uint16 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(0u128, false)
					} else {
						(s.parse::<u128>().unwrap_or(0), true)
					}
				})
				.unzip();
			FrameColumnData::Uint16(NumberContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Utf8 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(String::new(), false)
					} else {
						(s, true)
					}
				})
				.unzip();
			FrameColumnData::Utf8(Utf8Container::new(values, BitVec::from_slice(&defined)))
		}
		Type::Date => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(Date::from_ymd(1970, 1, 1).unwrap(), false)
					} else {
						let parts: Vec<&str> = s.split('-').collect();
						if parts.len() == 3 {
							let year = parts[0].parse::<i32>().unwrap_or(1970);
							let month = parts[1].parse::<u32>().unwrap_or(1);
							let day = parts[2].parse::<u32>().unwrap_or(1);
							(
								Date::from_ymd(year, month, day)
									.unwrap_or(Date::from_ymd(1970, 1, 1).unwrap()),
								true,
							)
						} else {
							(Date::from_ymd(1970, 1, 1).unwrap(), false)
						}
					}
				})
				.unzip();
			FrameColumnData::Date(TemporalContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::DateTime => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(DateTime::from_timestamp(0).unwrap(), false)
					} else if let Ok(dt) = parse_datetime(OwnedFragment::testing(&s)) {
						(dt, true)
					} else if let Ok(timestamp) = s.parse::<i64>() {
						(
							DateTime::from_timestamp(timestamp)
								.unwrap_or(DateTime::from_timestamp(0).unwrap()),
							true,
						)
					} else {
						(DateTime::from_timestamp(0).unwrap(), false)
					}
				})
				.unzip();
			FrameColumnData::DateTime(TemporalContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Time => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(Time::from_hms(0, 0, 0).unwrap(), false)
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
							(
								Time::from_hms_nano(hour, min, sec, nano)
									.unwrap_or(Time::from_hms(0, 0, 0).unwrap()),
								true,
							)
						} else {
							(Time::from_hms(0, 0, 0).unwrap(), false)
						}
					}
				})
				.unzip();
			FrameColumnData::Time(TemporalContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Duration => {
			// For Duration, store as Utf8 for now (needs proper ISO 8601 parsing)
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(String::new(), false)
					} else {
						(s, true)
					}
				})
				.unzip();
			FrameColumnData::Utf8(Utf8Container::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uuid4 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(parse_uuid4("00000000-0000-0000-0000-000000000000").unwrap(), false)
					} else if let Ok(uuid) = parse_uuid4(&s) {
						(uuid, true)
					} else {
						(parse_uuid4("00000000-0000-0000-0000-000000000000").unwrap(), false)
					}
				})
				.unzip();
			FrameColumnData::Uuid4(UuidContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Uuid7 => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(parse_uuid7("00000000-0000-7000-8000-000000000000").unwrap(), false)
					} else if let Ok(uuid) = parse_uuid7(&s) {
						(uuid, true)
					} else {
						(parse_uuid7("00000000-0000-7000-8000-000000000000").unwrap(), false)
					}
				})
				.unzip();
			FrameColumnData::Uuid7(UuidContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::IdentityId => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(
							IdentityId::from(Uuid7::from(
								parse_uuid7("00000000-0000-7000-8000-000000000000")
									.unwrap(),
							)),
							false,
						)
					} else if let Ok(uuid) = parse_uuid7(&s) {
						(IdentityId::from(Uuid7::from(uuid)), true)
					} else {
						(
							IdentityId::from(Uuid7::from(
								parse_uuid7("00000000-0000-7000-8000-000000000000")
									.unwrap(),
							)),
							false,
						)
					}
				})
				.unzip();
			FrameColumnData::IdentityId(IdentityIdContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Blob => {
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(Blob::new(vec![]), false)
					} else if s.starts_with("0x") {
						if let Ok(bytes) = hex::decode(&s[2..]) {
							(Blob::new(bytes), true)
						} else {
							(Blob::new(vec![]), false)
						}
					} else {
						(Blob::new(vec![]), false)
					}
				})
				.unzip();
			FrameColumnData::Blob(BlobContainer::new(values, BitVec::from_slice(&defined)))
		}
		Type::Int
		| Type::Uint
		| Type::Decimal {
			..
		}
		| Type::Any => {
			// For arbitrary-precision types and Any, store as Utf8
			let (values, defined): (Vec<_>, Vec<_>) = data
				.into_iter()
				.map(|s| {
					if s == "⟪undefined⟫" {
						(String::new(), false)
					} else {
						(s, true)
					}
				})
				.unzip();
			FrameColumnData::Utf8(Utf8Container::new(values, BitVec::from_slice(&defined)))
		}
	}
}
