// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// use crate::{ColumnCompressor, CompressedColumn, CompressionType};
// use reifydb_core::value::{column::ColumnData, container::BoolContainer};
// use reifydb_type::{Result, Type};
// use serde::{Deserialize, Serialize};
// use CompressionType::BitPacking;
//
// #[derive(Serialize, Deserialize)]
// struct BitPackedBool {
// 	packed: Vec<u8>,
// 	undefined_positions: Vec<usize>,
// 	total_count: usize,
// }
//
// pub struct BitPackCompressor;
//
// impl BitPackCompressor {
// 	pub fn new() -> Self {
// 		Self
// 	}
// }
//
// impl ColumnCompressor for BitPackCompressor {
// 	fn compress(&self, data: &ColumnData) -> Result<CompressedColumn> {
// 		assert_eq!(data.get_type(), Type::Boolean, "Can only compress boolean");
//
// 		let ColumnData::Bool(container) = data else {
// 			unreachable!()
// 		};
//
// 		compress_bool(container)
// 	}
//
// 	fn decompress(&self, compressed: &CompressedColumn) -> Result<ColumnData> {
// 		assert_eq!(compressed.compression, BitPacking);
//
// 		decompress_bool(&compressed.data, compressed.row_count)
// 	}
// }
//
// fn compress_bool(container: &BoolContainer) -> Result<CompressedColumn> {
// 	let mut packed_data = Vec::new();
// 	let mut undefined_positions = Vec::new();
// 	let mut current_byte = 0u8;
// 	let mut bit_position = 0;
//
// 	for (idx, value) in container.iter().enumerate() {
// 		match value {
// 			Some(true) => {
// 				current_byte |= 1 << bit_position;
// 			}
// 			Some(false) => {
// 				// Bit remains 0
// 			}
// 			None => {
// 				undefined_positions.push(idx);
// 			}
// 		}
//
// 		bit_position += 1;
// 		if bit_position == 8 {
// 			packed_data.push(current_byte);
// 			current_byte = 0;
// 			bit_position = 0;
// 		}
// 	}
//
// 	// Don't forget the last partial byte
// 	if bit_position > 0 {
// 		packed_data.push(current_byte);
// 	}
//
// 	let encoded = BitPackedBool {
// 		packed: packed_data,
// 		undefined_positions,
// 		total_count: container.len(),
// 	};
//
// 	let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	Ok(CompressedColumn {
// 		data,
// 		compression: BitPacking,
// 		uncompressed_size: container.len(), // 1 byte per bool in uncompressed form
// 		undefined_count: encoded.undefined_positions.len(),
// 		row_count: container.len(),
// 	})
// }
//
// fn decompress_bool(data: &[u8], _row_count: usize) -> Result<ColumnData> {
// 	let encoded: BitPackedBool =
// 		serde_json::from_slice(data).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	let mut result = Vec::with_capacity(encoded.total_count);
// 	let mut undefined_idx = 0;
// 	let mut packed_idx = 0;
// 	let mut bit_position = 0;
//
// 	for i in 0..encoded.total_count {
// 		if undefined_idx < encoded.undefined_positions.len() && encoded.undefined_positions[undefined_idx] == i
// 		{
// 			result.push(None);
// 			undefined_idx += 1;
// 		} else {
// 			if packed_idx >= encoded.packed.len() {
// 				// This shouldn't happen with correct encoding
// 				break;
// 			}
//
// 			let bit = (encoded.packed[packed_idx] >> bit_position) & 1;
// 			result.push(Some(bit == 1));
//
// 			bit_position += 1;
// 			if bit_position == 8 {
// 				packed_idx += 1;
// 				bit_position = 0;
// 			}
// 		}
// 	}
//
// 	Ok(ColumnData::bool_optional(result))
// }
