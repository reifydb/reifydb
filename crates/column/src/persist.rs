// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	data::{Column, canonical::Canonical},
};
use reifydb_value::{Result, error::Error, value::value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::{
	error::ColumnError,
	snapshot::{ColumnBlock, ColumnChunks},
};

const FORMAT_VERSION: u16 = 1;

#[derive(Serialize, Deserialize)]
struct PersistedColumn {
	chunks: Vec<ColumnBuffer>,
}

#[derive(Serialize, Deserialize)]
struct PersistedBlock {
	format_version: u16,
	schema: Vec<(String, ValueType, bool)>,
	columns: Vec<PersistedColumn>,
}

pub fn serialize_block(block: &ColumnBlock) -> Result<Vec<u8>> {
	let mut columns = Vec::with_capacity(block.columns.len());
	for column in &block.columns {
		let mut chunks = Vec::with_capacity(column.chunks.len());
		for chunk in &column.chunks {
			let canonical = chunk.to_canonical()?;
			chunks.push(canonical.to_buffer());
		}
		columns.push(PersistedColumn {
			chunks,
		});
	}

	let persisted = PersistedBlock {
		format_version: FORMAT_VERSION,
		schema: block.schema.as_ref().clone(),
		columns,
	};

	to_stdvec(&persisted).map_err(|e| {
		Error::from(ColumnError::PersistSerialize {
			reason: e.to_string(),
		})
	})
}

pub fn deserialize_block(bytes: &[u8]) -> Result<ColumnBlock> {
	let persisted: PersistedBlock = from_bytes(bytes).map_err(|e| {
		Error::from(ColumnError::PersistDeserialize {
			reason: e.to_string(),
		})
	})?;

	if persisted.format_version != FORMAT_VERSION {
		return Err(Error::from(ColumnError::PersistVersionUnsupported {
			version: persisted.format_version,
		}));
	}

	if persisted.columns.len() != persisted.schema.len() {
		return Err(Error::from(ColumnError::PersistDeserialize {
			reason: format!(
				"schema has {} columns but {} column payloads were stored",
				persisted.schema.len(),
				persisted.columns.len()
			),
		}));
	}

	let schema = Arc::new(persisted.schema);
	let mut columns = Vec::with_capacity(persisted.columns.len());
	for (index, persisted_column) in persisted.columns.into_iter().enumerate() {
		let (_, ty, nullable) = &schema[index];
		let mut chunks = Vec::with_capacity(persisted_column.chunks.len());
		for buffer in persisted_column.chunks {
			chunks.push(Column::from_canonical(Canonical::from_buffer(buffer)));
		}
		columns.push(ColumnChunks::new(ty.clone(), *nullable, chunks));
	}

	Ok(ColumnBlock::new(schema, columns))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::value::column::{
		buffer::ColumnBuffer,
		data::{Column, canonical::Canonical},
	};
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;
	use crate::snapshot::{ColumnBlock, ColumnChunks};

	fn col(buffer: ColumnBuffer) -> Column {
		Column::from_canonical(Canonical::from_column_buffer(&buffer).unwrap())
	}

	fn block_values(block: &ColumnBlock) -> Vec<Vec<Value>> {
		block.columns
			.iter()
			.map(|column| {
				let mut out = Vec::new();
				for chunk in &column.chunks {
					for i in 0..chunk.len() {
						out.push(chunk.get_value(i));
					}
				}
				out
			})
			.collect()
	}

	fn assert_round_trips(block: ColumnBlock) {
		let bytes = serialize_block(&block).unwrap();
		let restored = deserialize_block(&bytes).unwrap();
		assert_eq!(*block.schema, *restored.schema, "schema must survive the round trip");
		assert_eq!(block.len(), restored.len(), "row count must survive the round trip");
		assert_eq!(block_values(&block), block_values(&restored), "values must survive the round trip");
	}

	#[test]
	fn round_trips_fixed_width_column() {
		let schema = Arc::new(vec![("a".to_string(), ValueType::Int4, false)]);
		let column = ColumnChunks::single(ValueType::Int4, false, col(ColumnBuffer::int4([1i32, 2, 3, 4])));
		assert_round_trips(ColumnBlock::new(schema, vec![column]));
	}

	#[test]
	fn round_trips_varlen_column() {
		let schema = Arc::new(vec![("s".to_string(), ValueType::Utf8, false)]);
		let column = ColumnChunks::single(
			ValueType::Utf8,
			false,
			col(ColumnBuffer::utf8(["alpha", "bravo", "charlie"])),
		);
		assert_round_trips(ColumnBlock::new(schema, vec![column]));
	}

	#[test]
	fn round_trips_multi_column_block() {
		let schema = Arc::new(vec![
			("id".to_string(), ValueType::Uint8, false),
			("name".to_string(), ValueType::Utf8, false),
		]);
		let columns = vec![
			ColumnChunks::single(ValueType::Uint8, false, col(ColumnBuffer::uint8(vec![1u64, 2, 3]))),
			ColumnChunks::single(ValueType::Utf8, false, col(ColumnBuffer::utf8(["x", "y", "z"]))),
		];
		assert_round_trips(ColumnBlock::new(schema, columns));
	}

	#[test]
	fn round_trips_nullable_column_preserving_none_positions() {
		let mut buffer = ColumnBuffer::int4_with_capacity(4);
		buffer.push::<i32>(10);
		buffer.push_none();
		buffer.push::<i32>(30);
		buffer.push_none();
		let canonical = Canonical::from_column_buffer(&buffer).unwrap();
		assert!(canonical.nullable, "buffer with push_none must canonicalize to a nullable column");

		let column = ColumnChunks::single(ValueType::Int4, true, Column::from_canonical(canonical));
		let schema = Arc::new(vec![("a".to_string(), ValueType::Int4, true)]);
		let block = ColumnBlock::new(schema, vec![column]);

		let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();

		assert!(restored.columns[0].nullable, "nullability must survive the round trip");
		assert_eq!(block_values(&block), block_values(&restored));
		let chunk = &restored.columns[0].chunks[0];
		assert!(chunk.is_defined(0));
		assert!(!chunk.is_defined(1), "none at index 1 must be preserved");
		assert!(chunk.is_defined(2));
		assert!(!chunk.is_defined(3), "none at index 3 must be preserved");
	}

	#[test]
	fn deserialize_rejects_garbage_without_panicking() {
		assert!(deserialize_block(&[0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
	}
}
