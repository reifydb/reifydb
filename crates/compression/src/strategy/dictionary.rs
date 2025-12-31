// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// use serde::{Deserialize, Serialize};
// use std::collections::HashMap;
//
// use crate::{ColumnCompressor, CompressedColumn, CompressionType};
// use reifydb_core::value::{column::ColumnData, container::Utf8Container};
// use reifydb_type::Result;
// use reifydb_type::value::constraint::bytes::MaxBytes;
//
// #[derive(Serialize, Deserialize)]
// struct DictionaryEncoded {
// 	dictionary: Vec<String>,
// 	indices: Vec<Option<u32>>,
// 	max_bytes: Option<MaxBytes>,
// }
//
// pub struct DictionaryCompressor;
//
// impl DictionaryCompressor {
// 	pub fn new() -> Self {
// 		Self
// 	}
// }
//
// impl ColumnCompressor for DictionaryCompressor {
// 	fn compress(&self, data: &ColumnData) -> Result<CompressedColumn> {
// 		// match data {
// 		// 	ColumnData::Utf8 {
// 		// 		container,
// 		// 		max_bytes,
// 		// 	} => compress_utf8(container, *max_bytes),
// 		// 	_ => {
// 		// 		// For non-string types, fall back to no compression
// 		// 		// In a real implementation, we might handle other categorical types
// 		// 		// For non-string types, fall back to no compression
// 		// 		// In a real implementation, we might handle other categorical types
// 		// 		return Err(reifydb_type::Error::Internal(
// 		// 			"Dictionary compression only supports UTF8 columns".to_string(),
// 		// 		));
// 		// 		let uncompressed_size = serialized.len();
// 		//
// 		// 		Ok(CompressedColumn {
// 		// 			data: serialized,
// 		// 			compression: CompressionType::None,
// 		// 			uncompressed_size,
// 		// 			undefined_count: count_undefined(data),
// 		// 			row_count: data.len(),
// 		// 		})
// 		// 	}
// 		// }
// 		unimplemented!()
// 	}
//
// 	fn decompress(&self, compressed: &CompressedColumn) -> Result<ColumnData> {
// 		assert_eq!(compressed.compression, CompressionType::Dictionary);
// 		decompress_dictionary(&compressed.data)
// 	}
// }
//
// fn compress_utf8(container: &Utf8Container, max_bytes: MaxBytes) -> Result<CompressedColumn> {
// 	let mut dictionary = Vec::new();
// 	let mut value_to_index = HashMap::new();
// 	let mut indices = Vec::with_capacity(container.len());
// 	let mut undefined_count = 0;
//
// 	// Build dictionary and encode values as indices
// 	for value in container.iter() {
// 		match value {
// 			Some(s) => {
// 				let index = *value_to_index.entry(s.clone()).or_insert_with(|| {
// 					let idx = dictionary.len() as u32;
// 					dictionary.push(s.clone());
// 					idx
// 				});
// 				indices.push(Some(index));
// 			}
// 			None => {
// 				indices.push(None);
// 				undefined_count += 1;
// 			}
// 		}
// 	}
//
// 	// Serialize the dictionary and indices
// 	let encoded = DictionaryEncoded {
// 		dictionary,
// 		indices,
// 		max_bytes: max_bytes.0,
// 	};
//
// 	let data = serde_json::to_vec(&encoded).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	// Calculate uncompressed size (rough estimate)
// 	let uncompressed_size = container.iter()
//         .map(|v| v.as_ref().map_or(0, |s| s.len() + 4)) // 4 bytes for length prefix
//         .sum();
//
// 	Ok(CompressedColumn {
// 		data,
// 		compression: CompressionType::Dictionary,
// 		uncompressed_size,
// 		undefined_count,
// 		row_count: container.len(),
// 	})
// }
//
// fn decompress_dictionary(data: &[u8]) -> Result<ColumnData> {
// 	let encoded: DictionaryEncoded =
// 		serde_json::from_slice(data).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 	let mut values = Vec::with_capacity(encoded.indices.len());
//
// 	for index_opt in encoded.indices {
// 		match index_opt {
// 			Some(idx) => {
// 				let value = encoded.dictionary.get(idx as usize).ok_or_else(|| {
// 					reifydb_type::Error::Internal("Invalid dictionary index".to_string())
// 				})?;
// 				values.push(Some(value.clone()));
// 			}
// 			None => {
// 				values.push(None);
// 			}
// 		}
// 	}
//
// 	Ok(ColumnData::Utf8 {
// 		container: Utf8Container::from(values),
// 		max_bytes: MaxBytes(encoded.max_bytes),
// 	})
// }
//
// fn count_undefined(data: &ColumnData) -> usize {
// 	match data {
// 		ColumnData::Utf8 {
// 			container,
// 			..
// 		} => container.iter().filter(|v| v.is_none()).count(),
// 		ColumnData::Bool(container) => container.iter().filter(|v| v.is_none()).count(),
// 		ColumnData::Int4(container) => container.iter().filter(|v| v.is_none()).count(),
// 		ColumnData::Int8(container) => container.iter().filter(|v| v.is_none()).count(),
// 		// Add other types as needed
// 		_ => 0,
// 	}
// }
