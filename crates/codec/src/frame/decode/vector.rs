// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::vector::VectorContainer, frame::data::FrameColumnData, value_type::ValueType};

use super::column_type_from_code;
use crate::error::DecodeError;

pub(crate) fn decode_vector_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Option<Result<FrameColumnData, DecodeError>> {
	let ty = match column_type_from_code(type_code) {
		Ok(ty) => ty,
		Err(e) => return Some(Err(e)),
	};

	if !matches!(ty, ValueType::Vector(_)) {
		return None;
	}

	Some(decode_plain(row_count, data))
}

fn decode_plain(row_count: usize, data: &[u8]) -> Result<FrameColumnData, DecodeError> {
	if data.len() < 4 {
		return Err(DecodeError::InvalidData(
			"vector column is missing its 4-byte dimension header".to_string(),
		));
	}

	let dims = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
	if dims == 0 {
		return Err(DecodeError::InvalidData("vector column declares zero dimensions".to_string()));
	}

	let payload = &data[4..];
	let expected = row_count.saturating_mul(dims as usize).saturating_mul(4);
	if payload.len() != expected {
		return Err(DecodeError::InvalidData(format!(
			"vector column payload is {} bytes, expected {} for {} rows of {} dimensions",
			payload.len(),
			expected,
			row_count,
			dims
		)));
	}

	let values: Vec<f32> = payload
		.chunks_exact(4)
		.map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
		.collect();

	Ok(FrameColumnData::Vector(VectorContainer::new(dims, values)))
}
