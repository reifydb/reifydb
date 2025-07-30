// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{Column, ColumnData};
use crate::function::ScalarFunction;
use reifydb_core::OwnedSpan;
use reifydb_core::value::Blob;

pub struct BlobHex;

impl BlobHex {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarFunction for BlobHex {
    fn scalar(&self, columns: &[Column], row_count: usize) -> crate::Result<ColumnData> {
        let column = columns.get(0).unwrap();

        match &column.data() {
            ColumnData::Utf8(container) => {
                let mut result_data = Vec::with_capacity(container.data().len());

                for i in 0..row_count {
                    if container.is_defined(i) {
                        let hex_str = &container[i];
                        let blob = Blob::from_hex(OwnedSpan::testing(hex_str))?;
                        result_data.push(blob);
                    } else {
                        result_data.push(Blob::empty())
                    }
                }

                Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
            }
            _ => unimplemented!("BlobHex only supports text input"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnQualified;
    use reifydb_core::value::container::StringContainer;

    #[test]
    fn test_blob_hex_valid_input() {
        let function = BlobHex::new();

        let hex_data = vec!["deadbeef".to_string()];
        let bitvec = vec![true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_hex_empty_string() {
        let function = BlobHex::new();

        let hex_data = vec!["".to_string()];
        let bitvec = vec![true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[] as &[u8]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_hex_uppercase() {
        let function = BlobHex::new();

        let hex_data = vec!["DEADBEEF".to_string()];
        let bitvec = vec![true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_hex_mixed_case() {
        let function = BlobHex::new();

        let hex_data = vec!["DeAdBeEf".to_string()];
        let bitvec = vec![true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_hex_multiple_rows() {
        let function = BlobHex::new();

        let hex_data = vec!["ff".to_string(), "00".to_string(), "deadbeef".to_string()];
        let bitvec = vec![true, true, true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnData::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), &[0xff]);
            assert_eq!(container[1].as_bytes(), &[0x00]);
            assert_eq!(container[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_hex_with_null_data() {
        let function = BlobHex::new();

        let hex_data = vec!["ff".to_string(), "".to_string(), "deadbeef".to_string()];
        let bitvec = vec![true, false, true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnData::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(!container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), &[0xff]);
            assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
            assert_eq!(container[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_hex_invalid_input_should_error() {
        let function = BlobHex::new();

        let hex_data = vec!["invalid_hex".to_string()];
        let bitvec = vec![true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for invalid hex input");
    }

    #[test]
    fn test_blob_hex_odd_length_should_error() {
        let function = BlobHex::new();

        let hex_data = vec!["abc".to_string()];
        let bitvec = vec![true];
        let input_column = Column::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: ColumnData::Utf8(StringContainer::new(hex_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for odd length hex string");
    }
}
