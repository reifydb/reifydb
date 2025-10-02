// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
//
// use crate::{ColumnCompressor, CompressedColumn, CompressionType};
// use reifydb_core::value::column::ColumnData;
// use reifydb_type::{Result, Value};
// use serde::{Deserialize, Serialize};
//
// #[derive(Serialize, Deserialize)]
// struct RleEncoded {
// 	runs: Vec<Run>,
// 	data_type: ColumnType,
// }
//
// #[derive(Serialize, Deserialize)]
// struct Run {
// 	value: Value,
// 	count: u32,
// }
//
// #[derive(Clone, Copy, Serialize, Deserialize)]
// enum ColumnType {
// 	Bool,
// 	Int4,
// 	Int8,
// 	Utf8(Option<usize>),
// 	Other,
// }
//
// pub struct RleCompressor;
//
// impl RleCompressor {
// 	pub fn new() -> Self {
// 		Self
// 	}
// }
//
// impl ColumnCompressor for RleCompressor {
// 	fn compress(&self, data: &ColumnData) -> Result<CompressedColumn> {
// 		// RLE is best for data with many repeated values
// 		// For simplicity, we'll serialize with bincode for now
// 		// A real implementation would encode runs more efficiently
//
// 		let runs = encode_runs(data)?;
// 		// Use serde json for now to avoid bincode 2.0 complexity
// 		let data_bytes = serde_json::to_vec(&runs).map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		let uncompressed_size = estimate_uncompressed_size(data);
//
// 		Ok(CompressedColumn {
// 			data: data_bytes,
// 			compression: CompressionType::RunLength,
// 			uncompressed_size,
// 			undefined_count: count_undefined_in_runs(&runs),
// 			row_count: data.len(),
// 		})
// 	}
//
// 	fn decompress(&self, compressed: &CompressedColumn) -> Result<ColumnData> {
// 		assert_eq!(compressed.compression, CompressionType::RunLength);
//
// 		let runs: RleEncoded = serde_json::from_slice(&compressed.data)
// 			.map_err(|e| reifydb_type::Error::Internal(e.to_string()))?;
//
// 		decode_runs(runs)
// 	}
// }
//
// fn encode_runs(data: &ColumnData) -> Result<RleEncoded> {
// 	let values = column_to_values(data);
//
// 	if values.is_empty() {
// 		return Ok(RleEncoded {
// 			runs: vec![],
// 			data_type: get_column_type(data),
// 		});
// 	}
//
// 	let mut runs = Vec::new();
// 	let mut current_value = values[0].clone();
// 	let mut count = 1u32;
//
// 	for value in values.iter().skip(1) {
// 		if value == &current_value {
// 			count += 1;
// 		} else {
// 			runs.push(Run {
// 				value: current_value.clone(),
// 				count,
// 			});
// 			current_value = value.clone();
// 			count = 1;
// 		}
// 	}
//
// 	// Don't forget the last run
// 	runs.push(Run {
// 		value: current_value,
// 		count,
// 	});
//
// 	Ok(RleEncoded {
// 		runs,
// 		data_type: get_column_type(data),
// 	})
// }
//
// fn decode_runs(encoded: RleEncoded) -> Result<ColumnData> {
// 	let mut values = Vec::new();
//
// 	for run in encoded.runs {
// 		for _ in 0..run.count {
// 			values.push(run.value.clone());
// 		}
// 	}
//
// 	values_to_column(values, encoded.data_type)
// }
//
// fn column_to_values(data: &ColumnData) -> Vec<Value> {
// 	match data {
// 		ColumnData::Bool(container) => {
// 			container.iter().map(|v| v.map_or(Value::Undefined, Value::Boolean)).collect()
// 		}
// 		ColumnData::Int4(container) => {
// 			container.iter().map(|v| v.map_or(Value::Undefined, |i| Value::Int4(i))).collect()
// 		}
// 		ColumnData::Int8(container) => {
// 			container.iter().map(|v| v.map_or(Value::Undefined, |i| Value::Int8(i))).collect()
// 		}
// 		ColumnData::Utf8 {
// 			container,
// 			..
// 		} => container.iter().map(|v| v.map_or(Value::Undefined, |s| Value::Utf8(s))).collect(),
// 		// Add more types as needed
// 		_ => vec![],
// 	}
// }
//
// fn values_to_column(values: Vec<Value>, data_type: ColumnType) -> Result<ColumnData> {
// 	use reifydb_core::value::container::{BoolContainer, NumberContainer, Utf8Container};
//
// 	match data_type {
// 		ColumnType::Bool => {
// 			let bools: Vec<Option<bool>> = values
// 				.into_iter()
// 				.map(|v| match v {
// 					Value::Boolean(b) => Some(b),
// 					Value::Undefined => None,
// 					_ => None,
// 				})
// 				.collect();
// 			Ok(ColumnData::Bool(BoolContainer::from(bools)))
// 		}
// 		ColumnType::Int4 => {
// 			let ints: Vec<Option<i32>> = values
// 				.into_iter()
// 				.map(|v| match v {
// 					Value::Int4(i) => Some(i),
// 					Value::Undefined => None,
// 					_ => None,
// 				})
// 				.collect();
// 			Ok(ColumnData::Int4(NumberContainer::from(ints)))
// 		}
// 		ColumnType::Int8 => {
// 			let ints: Vec<Option<i64>> = values
// 				.into_iter()
// 				.map(|v| match v {
// 					Value::Int8(i) => Some(i),
// 					Value::Undefined => None,
// 					_ => None,
// 				})
// 				.collect();
// 			Ok(ColumnData::Int8(NumberContainer::from(ints)))
// 		}
// 		ColumnType::Utf8(max_bytes) => {
// 			let strings: Vec<Option<String>> = values
// 				.into_iter()
// 				.map(|v| match v {
// 					Value::Utf8(s) => Some(s),
// 					Value::Undefined => None,
// 					_ => None,
// 				})
// 				.collect();
// 			Ok(ColumnData::Utf8 {
// 				container: Utf8Container::from(strings),
// 				max_bytes: reifydb_type::value::constraint::bytes::MaxBytes(max_bytes),
// 			})
// 		}
// 		// Add more types as needed
// 		_ => Err(reifydb_type::Error::Internal("Unsupported column type for RLE".to_string())),
// 	}
// }
//
// fn get_column_type(data: &ColumnData) -> ColumnType {
// 	match data {
// 		ColumnData::Bool(_) => ColumnType::Bool,
// 		ColumnData::Int4(_) => ColumnType::Int4,
// 		ColumnData::Int8(_) => ColumnType::Int8,
// 		ColumnData::Utf8 {
// 			max_bytes,
// 			..
// 		} => ColumnType::Utf8(max_bytes.0),
// 		_ => ColumnType::Other,
// 	}
// }
//
// fn estimate_uncompressed_size(data: &ColumnData) -> usize {
// 	match data {
// 		ColumnData::Bool(_) => data.len(),
// 		ColumnData::Int4(_) => data.len() * 4,
// 		ColumnData::Int8(_) => data.len() * 8,
// 		ColumnData::Utf8 {
// 			container,
// 			..
// 		} => container.iter().map(|v| v.as_ref().map_or(0, |s| s.len() + 4)).sum(),
// 		_ => data.len() * 8, // Default estimate
// 	}
// }
//
// fn count_undefined_in_runs(runs: &RleEncoded) -> usize {
// 	runs.runs.iter().filter(|run| matches!(run.value, Value::Undefined)).map(|run| run.count as usize).sum()
// }
