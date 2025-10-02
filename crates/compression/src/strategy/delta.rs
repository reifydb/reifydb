// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
//
// use crate::{ColumnCompressor, CompressedColumn, CompressionType};
// use reifydb_core::value::{column::ColumnData, container::NumberContainer};
// use reifydb_type::Result;
// use serde::{Deserialize, Serialize};
//
// #[derive(Serialize, Deserialize)]
// enum DeltaEncoded {
// 	Int4 {
// 		base: i32,
// 		deltas: Vec<i32>,
// 		undefined: Vec<usize>,
// 	},
// 	Int8 {
// 		base: i64,
// 		deltas: Vec<i64>,
// 		undefined: Vec<usize>,
// 	},
// 	Uint4 {
// 		base: u32,
// 		deltas: Vec<i32>,
// 		undefined: Vec<usize>,
// 	},
// 	Uint8 {
// 		base: u64,
// 		deltas: Vec<i64>,
// 		undefined: Vec<usize>,
// 	},
// }
//
// pub struct DeltaCompressor;
//
// impl DeltaCompressor {
// 	pub fn new() -> Self {
// 		Self
// 	}
// }
//
// impl ColumnCompressor for DeltaCompressor {
// 	fn compress(&self, data: &ColumnData) -> Result<CompressedColumn> {
// 		let compressed_data = match data {
// 			ColumnData::Int4(container) => compress_i32(container)?,
// 			ColumnData::Int8(container) => compress_i64(container)?,
// 			ColumnData::Uint4(container) => compress_u32(container)?,
// 			ColumnData::Uint8(container) => compress_u64(container)?,
// 			// For other types, use simple serialization
// 			_ => {
// 				return Err(reifydb_type::Error::Internal(
// 					"Delta compression only supports integer types".to_string(),
// 				));
// 			}
// 		};
//
// 		Ok(compressed_data)
// 	}
//
// 	fn decompress(&self, compressed: &CompressedColumn) -> Result<ColumnData> {
// 		if compressed.compression != CompressionType::Delta {
// 			// Handle non-delta compressed data
// 			return Err(reifydb_type::Error::Internal("Cannot decompress non-delta data".to_string()));
// 		}
//
// 		// Deserialize the delta encoded data
// 		let encoded: DeltaEncoded = serde_json::from_slice(&compressed.data)
// 			.map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		match encoded {
// 			DeltaEncoded::Int4 {
// 				base,
// 				deltas,
// 				undefined,
// 			} => Ok(ColumnData::Int4(decompress_i32(base, deltas, undefined))),
// 			DeltaEncoded::Int8 {
// 				base,
// 				deltas,
// 				undefined,
// 			} => Ok(ColumnData::Int8(decompress_i64(base, deltas, undefined))),
// 			DeltaEncoded::Uint4 {
// 				base,
// 				deltas,
// 				undefined,
// 			} => Ok(ColumnData::Uint4(decompress_u32(base, deltas, undefined))),
// 			DeltaEncoded::Uint8 {
// 				base,
// 				deltas,
// 				undefined: undefined,
// 			} => Ok(ColumnData::Uint8(decompress_u64(base, deltas, undefined))),
// 		}
// 	}
//
// 	fn compression_type(&self) -> CompressionType {
// 		CompressionType::Delta
// 	}
// }
//
// fn compress_i32(container: &NumberContainer<i32>) -> Result<CompressedColumn> {
// 	let mut values = Vec::new();
// 	let mut undefined_positions = Vec::new();
//
// 	for (i, val) in container.iter().enumerate() {
// 		match val {
// 			Some(v) => values.push(*v),
// 			None => undefined_positions.push(i),
// 		}
// 	}
//
// 	if values.is_empty() {
// 		// All undefined
// 		let encoded = DeltaEncoded::Int4 {
// 			base: 0,
// 			deltas: vec![],
// 			undefined: undefined_positions,
// 		};
// 		let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		return Ok(CompressedColumn {
// 			data,
// 			compression: CompressionType::Delta,
// 			uncompressed_size: container.len() * 4,
// 			undefined_count: undefined_positions.len(),
// 			row_count: container.len(),
// 		});
// 	}
//
// 	let base = values[0];
// 	let mut deltas = Vec::with_capacity(values.len() - 1);
//
// 	for i in 1..values.len() {
// 		deltas.push(values[i] - values[i - 1]);
// 	}
//
// 	let encoded = DeltaEncoded::Int4 {
// 		base,
// 		deltas,
// 		undefined: undefined_positions,
// 	};
//
// 	let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	Ok(CompressedColumn {
// 		data,
// 		compression: CompressionType::Delta,
// 		uncompressed_size: container.len() * 4,
// 		undefined_count: undefined_positions.len(),
// 		row_count: container.len(),
// 	})
// }
//
// fn compress_i64(container: &NumberContainer<i64>) -> Result<CompressedColumn> {
// 	let mut values = Vec::new();
// 	let mut undefined_positions = Vec::new();
//
// 	for (i, val) in container.iter().enumerate() {
// 		match val {
// 			Some(v) => values.push(*v),
// 			None => undefined_positions.push(i),
// 		}
// 	}
//
// 	if values.is_empty() {
// 		let encoded = DeltaEncoded::Int8 {
// 			base: 0,
// 			deltas: vec![],
// 			undefined: undefined_positions,
// 		};
// 		let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		return Ok(CompressedColumn {
// 			data,
// 			compression: CompressionType::Delta,
// 			uncompressed_size: container.len() * 8,
// 			undefined_count: undefined_positions.len(),
// 			row_count: container.len(),
// 		});
// 	}
//
// 	let base = values[0];
// 	let mut deltas = Vec::with_capacity(values.len() - 1);
//
// 	for i in 1..values.len() {
// 		deltas.push(values[i] - values[i - 1]);
// 	}
//
// 	let encoded = DeltaEncoded::Int8 {
// 		base,
// 		deltas,
// 		undefined: undefined_positions,
// 	};
//
// 	let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	Ok(CompressedColumn {
// 		data,
// 		compression: CompressionType::Delta,
// 		uncompressed_size: container.len() * 8,
// 		undefined_count: undefined_positions.len(),
// 		row_count: container.len(),
// 	})
// }
//
// fn compress_u32(container: &NumberContainer<u32>) -> Result<CompressedColumn> {
// 	let mut values = Vec::new();
// 	let mut undefined_positions = Vec::new();
//
// 	for (i, val) in container.iter().enumerate() {
// 		match val {
// 			Some(v) => values.push(*v),
// 			None => undefined_positions.push(i),
// 		}
// 	}
//
// 	if values.is_empty() {
// 		let encoded = DeltaEncoded::Uint4 {
// 			base: 0,
// 			deltas: vec![],
// 			undefined: undefined_positions,
// 		};
// 		let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		return Ok(CompressedColumn {
// 			data,
// 			compression: CompressionType::Delta,
// 			uncompressed_size: container.len() * 4,
// 			undefined_count: undefined_positions.len(),
// 			row_count: container.len(),
// 		});
// 	}
//
// 	let base = values[0];
// 	let mut deltas = Vec::with_capacity(values.len() - 1);
//
// 	for i in 1..values.len() {
// 		deltas.push((values[i] as i32) - (values[i - 1] as i32));
// 	}
//
// 	let encoded = DeltaEncoded::Uint4 {
// 		base,
// 		deltas,
// 		undefined: undefined_positions,
// 	};
//
// 	let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	Ok(CompressedColumn {
// 		data,
// 		compression: CompressionType::Delta,
// 		uncompressed_size: container.len() * 4,
// 		undefined_count: undefined_positions.len(),
// 		row_count: container.len(),
// 	})
// }
//
// fn compress_u64(container: &NumberContainer<u64>) -> Result<CompressedColumn> {
// 	let mut values = Vec::new();
// 	let mut undefined_positions = Vec::new();
//
// 	for (i, val) in container.iter().enumerate() {
// 		match val {
// 			Some(v) => values.push(*v),
// 			None => undefined_positions.push(i),
// 		}
// 	}
//
// 	if values.is_empty() {
// 		let encoded = DeltaEncoded::Uint8 {
// 			base: 0,
// 			deltas: vec![],
// 			undefined: undefined_positions,
// 		};
// 		let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		return Ok(CompressedColumn {
// 			data,
// 			compression: CompressionType::Delta,
// 			uncompressed_size: container.len() * 8,
// 			undefined_count: undefined_positions.len(),
// 			row_count: container.len(),
// 		});
// 	}
//
// 	let base = values[0];
// 	let mut deltas = Vec::with_capacity(values.len() - 1);
//
// 	for i in 1..values.len() {
// 		deltas.push((values[i] as i64) - (values[i - 1] as i64));
// 	}
//
// 	let encoded = DeltaEncoded::Uint8 {
// 		base,
// 		deltas,
// 		undefined: undefined_positions,
// 	};
//
// 	let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	Ok(CompressedColumn {
// 		data,
// 		compression: CompressionType::Delta,
// 		uncompressed_size: container.len() * 8,
// 		undefined_count: undefined_positions.len(),
// 		row_count: container.len(),
// 	})
// }
//
// fn decompress_i32(base: i32, deltas: Vec<i32>, undefined: Vec<usize>) -> NumberContainer<i32> {
// 	let total_len = deltas.len() + 1 + undefined.len();
// 	let mut result = Vec::with_capacity(total_len);
// 	let mut value_idx = 0;
// 	let mut undefined_idx = 0;
// 	let mut current_value = base;
//
// 	for i in 0..total_len {
// 		if undefined_idx < undefined.len() && undefined[undefined_idx] == i {
// 			result.push(None);
// 			undefined_idx += 1;
// 		} else {
// 			result.push(Some(current_value));
// 			if value_idx < deltas.len() {
// 				current_value += deltas[value_idx];
// 				value_idx += 1;
// 			}
// 		}
// 	}
//
// 	NumberContainer::from(result)
// }
//
// fn decompress_i64(base: i64, deltas: Vec<i64>, undefined: Vec<usize>) -> NumberContainer<i64> {
// 	let total_len = deltas.len() + 1 + undefined.len();
// 	let mut result = Vec::with_capacity(total_len);
// 	let mut value_idx = 0;
// 	let mut undefined_idx = 0;
// 	let mut current_value = base;
//
// 	for i in 0..total_len {
// 		if undefined_idx < undefined.len() && undefined[undefined_idx] == i {
// 			result.push(None);
// 			undefined_idx += 1;
// 		} else {
// 			result.push(Some(current_value));
// 			if value_idx < deltas.len() {
// 				current_value += deltas[value_idx];
// 				value_idx += 1;
// 			}
// 		}
// 	}
//
// 	NumberContainer::from(result)
// }
//
// fn decompress_u32(base: u32, deltas: Vec<i32>, undefined: Vec<usize>) -> NumberContainer<u32> {
// 	let total_len = deltas.len() + 1 + undefined.len();
// 	let mut result = Vec::with_capacity(total_len);
// 	let mut value_idx = 0;
// 	let mut undefined_idx = 0;
// 	let mut current_value = base as i32;
//
// 	for i in 0..total_len {
// 		if undefined_idx < undefined.len() && undefined[undefined_idx] == i {
// 			result.push(None);
// 			undefined_idx += 1;
// 		} else {
// 			result.push(Some(current_value as u32));
// 			if value_idx < deltas.len() {
// 				current_value += deltas[value_idx];
// 				value_idx += 1;
// 			}
// 		}
// 	}
//
// 	NumberContainer::from(result)
// }
//
// fn decompress_u64(base: u64, deltas: Vec<i64>, undefined: Vec<usize>) -> NumberContainer<u64> {
// 	let total_len = deltas.len() + 1 + undefined.len();
// 	let mut result = Vec::with_capacity(total_len);
// 	let mut value_idx = 0;
// 	let mut undefined_idx = 0;
// 	let mut current_value = base as i64;
//
// 	for i in 0..total_len {
// 		if undefined_idx < undefined.len() && undefined[undefined_idx] == i {
// 			result.push(None);
// 			undefined_idx += 1;
// 		} else {
// 			result.push(Some(current_value as u64));
// 			if value_idx < deltas.len() {
// 				current_value += deltas[value_idx];
// 				value_idx += 1;
// 			}
// 		}
// 	}
//
// 	NumberContainer::from(result)
// }
