// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::ColumnData;

use crate::{BoxedColumnCompressor, strategy::none::NoneCompressor};

/// Select the best compressor for a given column based on its type and characteristics
pub fn select_compressor(_data: &ColumnData) -> BoxedColumnCompressor {
	// match data.get_type() {
	// 	Type::Boolean => Box::new(BitPackCompressor::new()),
	// 	Type::Utf8 => {
	// 		// For now, always use dictionary for strings
	// 		// TODO: Add cardinality check to decide between dictionary and zstd
	// 		Box::new(DictionaryCompressor::new())
	// 	}
	// 	Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16 => {
	// 		// For now, use delta for integers
	// 		// TODO: Check if sorted to decide between delta and zstd
	// 		Box::new(DeltaCompressor::new())
	// 	}
	// 	Type::Uint1 | Type::Uint2 | Type::Uint4 | Type::Uint8 | Type::Uint16 => {
	// 		Box::new(DeltaCompressor::new())
	// 	}
	// 	Type::Float4 | Type::Float8 => {
	// 		// Floating point typically doesn't compress well with delta
	// 		unimplemented!()
	// 	}
	// 	Type::Date | Type::DateTime | Type::Time => {
	// 		// Temporal data often has patterns
	// 		Box::new(DeltaCompressor::new())
	// 	}
	// 	_ => unimplemented!(),
	// }
	Box::new(NoneCompressor {})
}
