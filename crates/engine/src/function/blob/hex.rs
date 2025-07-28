// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::function::ScalarFunction;
use reifydb_core::OwnedSpan;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use reifydb_core::value::Blob;

pub struct BlobHex;

impl BlobHex {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarFunction for BlobHex {
    fn scalar(&self, columns: &[FrameColumn], row_count: usize) -> crate::Result<ColumnValues> {
        let column = columns.get(0).unwrap();

        match &column.values() {
            ColumnValues::Utf8(values, bitvec) => {
                let mut result_values = Vec::with_capacity(values.len());

                for i in 0..row_count {
                    if bitvec.get(i) {
                        let hex_str = &values[i];
                        let blob = Blob::from_hex(OwnedSpan::testing(hex_str))?;
                        result_values.push(blob);
                    } else {
                        result_values.push(Blob::empty())
                    }
                }

                Ok(ColumnValues::blob_with_bitvec(result_values, bitvec.clone()))
            }
            _ => unimplemented!("BlobHex only supports text input"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::frame::{ColumnQualified, FrameColumn};
    use reifydb_core::{BitVec, CowVec};

    #[test]
    fn test_blob_hex_valid_input() {
        let function = BlobHex::new();

        let hex_values = vec!["deadbeef".to_string()];
        let bitvec = BitVec::from_slice(&[true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(blobs, bitvec) = result {
            assert_eq!(blobs.len(), 1);
            assert_eq!(bitvec.get(0), true);
            assert_eq!(blobs[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_empty_string() {
        let function = BlobHex::new();

        let hex_values = vec!["".to_string()];
        let bitvec = BitVec::from_slice(&[true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(blobs, bitvec) = result {
            assert_eq!(blobs.len(), 1);
            assert_eq!(bitvec.get(0), true);
            assert_eq!(blobs[0].as_bytes(), &[] as &[u8]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_uppercase() {
        let function = BlobHex::new();

        let hex_values = vec!["DEADBEEF".to_string()];
        let bitvec = BitVec::from_slice(&[true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(blobs, bitvec) = result {
            assert_eq!(blobs.len(), 1);
            assert_eq!(bitvec.get(0), true);
            assert_eq!(blobs[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_mixed_case() {
        let function = BlobHex::new();

        let hex_values = vec!["DeAdBeEf".to_string()];
        let bitvec = BitVec::from_slice(&[true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(blobs, bitvec) = result {
            assert_eq!(blobs.len(), 1);
            assert_eq!(bitvec.get(0), true);
            assert_eq!(blobs[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_multiple_rows() {
        let function = BlobHex::new();

        let hex_values = vec!["ff".to_string(), "00".to_string(), "deadbeef".to_string()];
        let bitvec = BitVec::from_slice(&[true, true, true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnValues::Blob(blobs, bitvec) = result {
            assert_eq!(blobs.len(), 3);
            assert_eq!(bitvec.get(0), true);
            assert_eq!(bitvec.get(1), true);
            assert_eq!(bitvec.get(2), true);

            assert_eq!(blobs[0].as_bytes(), &[0xff]);
            assert_eq!(blobs[1].as_bytes(), &[0x00]);
            assert_eq!(blobs[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_with_null_values() {
        let function = BlobHex::new();

        let hex_values = vec!["ff".to_string(), "".to_string(), "deadbeef".to_string()];
        let bitvec = BitVec::from_slice(&[true, false, true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnValues::Blob(blobs, bitvec) = result {
            assert_eq!(blobs.len(), 3);
            assert_eq!(bitvec.get(0), true);
            assert_eq!(bitvec.get(1), false);
            assert_eq!(bitvec.get(2), true);

            assert_eq!(blobs[0].as_bytes(), &[0xff]);
            assert_eq!(blobs[1].as_bytes(), [].as_slice() as &[u8]);
            assert_eq!(blobs[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_invalid_input_should_error() {
        let function = BlobHex::new();

        let hex_values = vec!["invalid_hex".to_string()];
        let bitvec = BitVec::from_slice(&[true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for invalid hex input");
    }

    #[test]
    fn test_blob_hex_odd_length_should_error() {
        let function = BlobHex::new();

        let hex_values = vec!["abc".to_string()];
        let bitvec = BitVec::from_slice(&[true]);
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(CowVec::new(hex_values), bitvec.clone()),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for odd length hex string");
    }
}
