// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

use crate::array::Array;

// A column as a sequence of `Array` chunks, each encoded independently. v1
// materialization produces single-chunk `ChunkedArray`s; multi-chunk support
// is reserved for the future batched-scan path.
#[derive(Clone)]
pub struct ChunkedArray {
	pub ty: Type,
	pub nullable: bool,
	pub chunks: Vec<Array>,
}

impl ChunkedArray {
	pub fn new(ty: Type, nullable: bool, chunks: Vec<Array>) -> Self {
		Self {
			ty,
			nullable,
			chunks,
		}
	}

	pub fn single(ty: Type, nullable: bool, array: Array) -> Self {
		Self {
			ty,
			nullable,
			chunks: vec![array],
		}
	}

	pub fn len(&self) -> usize {
		self.chunks.iter().map(|c| c.len()).sum()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn chunk_count(&self) -> usize {
		self.chunks.len()
	}
}
