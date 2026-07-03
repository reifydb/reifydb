// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum EncodeError {
	#[error("unsupported type: {0}")]
	UnsupportedType(String),

	#[error("option nesting depth {depth} exceeds the maximum of {max}")]
	OptionDepthTooDeep {
		depth: u32,
		max: u8,
	},
}

#[derive(Debug, Error, PartialEq)]
pub enum DecodeError {
	#[error("unexpected EOF: expected {expected} bytes, got {available}")]
	UnexpectedEof {
		expected: usize,
		available: usize,
	},

	#[error("invalid magic: 0x{0:08X}")]
	InvalidMagic(u32),

	#[error("unsupported version: {0}")]
	UnsupportedVersion(u16),

	#[error("unknown type code: {0}")]
	UnknownTypeCode(u8),

	#[error("unknown encoding: {0}")]
	UnknownEncoding(u8),

	#[error("reserved tag byte: 0x{0:02X}")]
	ReservedTag(u8),

	#[error("trailing bytes: {0} remaining after decode")]
	TrailingBytes(usize),

	#[error("invalid data: {0}")]
	InvalidData(String),

	#[error("unsupported type: {0}")]
	UnsupportedType(String),

	#[error("column '{column_name}' decode failed{}: {source}", row_index.map(|r| format!(" at row {}", r)).unwrap_or_default())]
	ColumnDecodeFailed {
		column_name: String,
		row_index: Option<usize>,
		source: Box<DecodeError>,
	},
}
