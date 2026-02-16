// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	err,
	error::Error,
	fragment::Fragment,
	util::{bitvec::BitVec, hex},
	value::{
		blob::Blob,
		container::{
			blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, undefined::UndefinedContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		row_number::RowNumber,
		temporal::parse::datetime::parse_datetime,
		time::Time,
		r#type::Type,
		uuid::{
			Uuid7,
			parse::{parse_uuid4, parse_uuid7},
		},
	},
};

/// Result type for admin operations
#[derive(Debug)]
pub struct AdminResult {
	pub frames: Vec<Frame>,
}

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
pub fn parse_admin_response(response: crate::Response) -> Result<AdminResult, Error> {
	match response.payload {
		crate::ResponsePayload::Admin(admin_response) => Ok(AdminResult {
			frames: convert_admin_response(admin_response),
		}),
		// Admin responses may come back as Command responses from the server
		crate::ResponsePayload::Command(cmd_response) => Ok(AdminResult {
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

pub fn convert_admin_response(payload: crate::AdminResponse) -> Vec<Frame> {
	let mut result = Vec::new();

	for frame in payload.frames {
		let columns = frame
			.columns
			.into_iter()
			.map(|col| FrameColumn {
				name: col.name,
				data: convert_column_to_data(col.r#type, col.data),
			})
			.collect();

		let row_numbers = frame.row_numbers.into_iter().map(RowNumber::new).collect();
		result.push(Frame::with_row_numbers(columns, row_numbers))
	}

	result
}

pub fn convert_command_response(payload: crate::CommandResponse) -> Vec<Frame> {
	let mut result = Vec::new();

	for frame in payload.frames {
		let columns = frame
			.columns
			.into_iter()
			.map(|col| FrameColumn {
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
		Type::Option(inner_type) => {
			let defined: Vec<bool> = data.iter().map(|s| s != "⟪none⟫").collect();

			// All none → short-circuit
			if defined.iter().all(|&b| !b) {
				return FrameColumnData::Undefined(UndefinedContainer::new(len));
			}

			let bitvec = BitVec::from_slice(&defined);
			let inner = convert_column_to_data(*inner_type, data);

			// All defined → return bare inner (fast path)
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
			// For Duration, store as Utf8 for now (needs proper ISO 8601 parsing)
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
		Type::Uuid4 => {
			let values: Vec<_> = data
				.into_iter()
				.map(|s| {
					if s == "⟪none⟫" {
						parse_uuid4("00000000-0000-0000-0000-000000000000".into()).unwrap()
					} else if let Ok(uuid) = parse_uuid4(s.into()) {
						uuid
					} else {
						parse_uuid4("00000000-0000-0000-0000-000000000000".into()).unwrap()
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
						IdentityId::from(Uuid7::from(
							parse_uuid7("00000000-0000-7000-8000-000000000000".into())
								.unwrap(),
						))
					} else if let Ok(uuid) = parse_uuid7(s.into()) {
						IdentityId::from(Uuid7::from(uuid))
					} else {
						IdentityId::from(Uuid7::from(
							parse_uuid7("00000000-0000-7000-8000-000000000000".into())
								.unwrap(),
						))
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
					} else if s.starts_with("0x") {
						if let Ok(bytes) = hex::decode(&s[2..]) {
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
		Type::Int
		| Type::Uint
		| Type::Decimal {
			..
		}
		| Type::Any
		| Type::DictionaryId => {
			// For arbitrary-precision types, Any, and DictionaryId, store as Utf8
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
