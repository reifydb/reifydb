// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Error types for RBCF encoding and decoding.

use std::{error, fmt};

/// Errors that can occur during encoding.
#[derive(Debug)]
pub enum EncodeError {
	/// A type that cannot be encoded in RBCF (e.g. List, Record, Tuple).
	UnsupportedType(String),
}

impl fmt::Display for EncodeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			EncodeError::UnsupportedType(ty) => write!(f, "unsupported type: {}", ty),
		}
	}
}

impl error::Error for EncodeError {}

/// Errors that can occur during decoding.
#[derive(Debug)]
pub enum DecodeError {
	/// Not enough bytes to read the expected data.
	UnexpectedEof {
		expected: usize,
		available: usize,
	},
	/// Invalid magic bytes.
	InvalidMagic(u32),
	/// Unsupported format version.
	UnsupportedVersion(u16),
	/// Unknown type code.
	UnknownTypeCode(u8),
	/// Unknown encoding.
	UnknownEncoding(u8),
	/// Invalid data (e.g., invalid UTF-8, invalid date).
	InvalidData(String),
	/// A type that cannot be decoded from RBCF (e.g. List, Record, Tuple).
	UnsupportedType(String),
	/// Wraps an inner error with column context.
	ColumnDecodeFailed {
		column_name: String,
		row_index: Option<usize>,
		source: Box<DecodeError>,
	},
}

impl fmt::Display for DecodeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			DecodeError::UnexpectedEof {
				expected,
				available,
			} => {
				write!(f, "unexpected EOF: expected {} bytes, got {}", expected, available)
			}
			DecodeError::InvalidMagic(m) => write!(f, "invalid magic: 0x{:08X}", m),
			DecodeError::UnsupportedVersion(v) => write!(f, "unsupported version: {}", v),
			DecodeError::UnknownTypeCode(c) => write!(f, "unknown type code: {}", c),
			DecodeError::UnknownEncoding(e) => write!(f, "unknown encoding: {}", e),
			DecodeError::InvalidData(msg) => write!(f, "invalid data: {}", msg),
			DecodeError::UnsupportedType(ty) => write!(f, "unsupported type: {}", ty),
			DecodeError::ColumnDecodeFailed {
				column_name,
				row_index,
				source,
			} => {
				write!(f, "column '{}' decode failed", column_name)?;
				if let Some(row) = row_index {
					write!(f, " at row {}", row)?;
				}
				write!(f, ": {}", source)
			}
		}
	}
}

impl error::Error for DecodeError {
	fn source(&self) -> Option<&(dyn error::Error + 'static)> {
		match self {
			DecodeError::ColumnDecodeFailed {
				source,
				..
			} => Some(source.as_ref()),
			_ => None,
		}
	}
}
