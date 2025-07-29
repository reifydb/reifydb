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
            ColumnValues::Utf8(container) => {
                let mut result_values = Vec::with_capacity(container.values().len());

                for i in 0..row_count {
                    if container.is_defined(i) {
                        let hex_str = &container[i];
                        let blob = Blob::from_hex(OwnedSpan::testing(hex_str))?;
                        result_values.push(blob);
                    } else {
                        result_values.push(Blob::empty())
                    }
                }

                Ok(ColumnValues::blob_with_bitvec(result_values, container.bitvec().clone()))
            }
            _ => unimplemented!("BlobHex only supports text input"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::frame::{ColumnQualified, FrameColumn};
    use reifydb_core::frame::column::container::StringContainer;

    #[test]
    fn test_blob_hex_valid_input() {
        let function = BlobHex::new();

        let hex_values = vec!["deadbeef".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_empty_string() {
        let function = BlobHex::new();

        let hex_values = vec!["".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[] as &[u8]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_uppercase() {
        let function = BlobHex::new();

        let hex_values = vec!["DEADBEEF".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_mixed_case() {
        let function = BlobHex::new();

        let hex_values = vec!["DeAdBeEf".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_multiple_rows() {
        let function = BlobHex::new();

        let hex_values = vec!["ff".to_string(), "00".to_string(), "deadbeef".to_string()];
        let bitvec = vec![true, true, true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), &[0xff]);
            assert_eq!(container[1].as_bytes(), &[0x00]);
            assert_eq!(container[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_with_null_values() {
        let function = BlobHex::new();

        let hex_values = vec!["ff".to_string(), "".to_string(), "deadbeef".to_string()];
        let bitvec = vec![true, false, true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(!container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), &[0xff]);
            assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
            assert_eq!(container[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_hex_invalid_input_should_error() {
        let function = BlobHex::new();

        let hex_values = vec!["invalid_hex".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for invalid hex input");
    }

    #[test]
    fn test_blob_hex_odd_length_should_error() {
        let function = BlobHex::new();

        let hex_values = vec!["abc".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(hex_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for odd length hex string");
    }
}