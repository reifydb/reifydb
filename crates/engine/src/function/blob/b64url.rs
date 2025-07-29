// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::function::ScalarFunction;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use reifydb_core::value::Blob;
use reifydb_core::OwnedSpan;

pub struct BlobB64url;

impl BlobB64url {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarFunction for BlobB64url {
    fn scalar(&self, columns: &[FrameColumn], row_count: usize) -> crate::Result<ColumnValues> {
        let column = columns.get(0).unwrap();

        match &column.values() {
            ColumnValues::Utf8(container) => {
                let mut result_values = Vec::with_capacity(container.values().len());

                for i in 0..row_count {
                    if container.is_defined(i) {
                        let b64url_str = &container[i];
                        let blob = Blob::from_b64url(OwnedSpan::testing(b64url_str))?;
                        result_values.push(blob);
                    } else {
                        result_values.push(Blob::empty())
                    }
                }

                Ok(ColumnValues::blob_with_bitvec(result_values, container.bitvec().clone()))
            }
            _ => unimplemented!("BlobB64url only supports text input"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::frame::column::container::StringContainer;
    use reifydb_core::frame::{ColumnQualified, FrameColumn};

    #[test]
    fn test_blob_b64url_valid_input() {
        let function = BlobB64url::new();

        // "Hello!" in base64url is "SGVsbG8h" (no padding needed)
        let b64url_values = vec!["SGVsbG8h".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), "Hello!".as_bytes());
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_b64url_empty_string() {
        let function = BlobB64url::new();

        let b64url_values = vec!["".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
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
    fn test_blob_b64url_url_safe_characters() {
        let function = BlobB64url::new();

        // Base64url uses - and _ instead of + and /
        // This string contains URL-safe characters
        let b64url_values = vec!["SGVsbG9fV29ybGQtSGVsbG8".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), "Hello_World-Hello".as_bytes());
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_b64url_no_padding() {
        let function = BlobB64url::new();

        // Base64url typically omits padding characters
        // "Hello" in base64url without padding is "SGVsbG8"
        let b64url_values = vec!["SGVsbG8".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), "Hello".as_bytes());
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_b64url_multiple_rows() {
        let function = BlobB64url::new();

        // "A" = "QQ", "BC" = "QkM", "DEF" = "REVG" (no padding in base64url)
        let b64url_values = vec!["QQ".to_string(), "QkM".to_string(), "REVG".to_string()];
        let bitvec = vec![true, true, true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), "A".as_bytes());
            assert_eq!(container[1].as_bytes(), "BC".as_bytes());
            assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_b64url_with_null_values() {
        let function = BlobB64url::new();

        let b64url_values = vec!["QQ".to_string(), "".to_string(), "REVG".to_string()];
        let bitvec = vec![true, false, true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let ColumnValues::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(!container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), "A".as_bytes());
            assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
            assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
        } else {
            panic!("Expected BLOB column values");
        }
    }

    #[test]
    fn test_blob_b64url_binary_data() {
        let function = BlobB64url::new();

        // Binary data: [0xde, 0xad, 0xbe, 0xef] in base64url is "3q2-7w" (no padding)
        let b64url_values = vec!["3q2-7w".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
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
    fn test_blob_b64url_invalid_input_should_error() {
        let function = BlobB64url::new();

        // Using standard base64 characters that are invalid in base64url
        let b64url_values = vec!["invalid+base64/chars".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for invalid base64url input");
    }

    #[test]
    fn test_blob_b64url_with_standard_base64_padding_should_error() {
        let function = BlobB64url::new();

        // Base64url typically doesn't use padding, so this should error
        let b64url_values = vec!["SGVsbG8=".to_string()];
        let bitvec = vec![true];
        let input_column = FrameColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            values: ColumnValues::Utf8(StringContainer::new(b64url_values, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for base64url with padding characters");
    }
}
