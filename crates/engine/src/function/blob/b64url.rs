// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{EngineColumn, EngineColumnData};
use crate::function::ScalarFunction;
use reifydb_core::OwnedSpan;
use reifydb_core::value::Blob;

pub struct BlobB64url;

impl BlobB64url {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarFunction for BlobB64url {
    fn scalar(
        &self,
        columns: &[EngineColumn],
        row_count: usize,
    ) -> crate::Result<EngineColumnData> {
        let column = columns.get(0).unwrap();

        match &column.data() {
            EngineColumnData::Utf8(container) => {
                let mut result_data = Vec::with_capacity(container.data().len());

                for i in 0..row_count {
                    if container.is_defined(i) {
                        let b64url_str = &container[i];
                        let blob = Blob::from_b64url(OwnedSpan::testing(b64url_str))?;
                        result_data.push(blob);
                    } else {
                        result_data.push(Blob::empty())
                    }
                }

                Ok(EngineColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
            }
            _ => unimplemented!("BlobB64url only supports text input"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnQualified;
    use crate::column::container::StringContainer;

    #[test]
    fn test_blob_b64url_valid_input() {
        let function = BlobB64url::new();

        // "Hello!" in base64url is "SGVsbG8h" (no padding needed)
        let b64url_data = vec!["SGVsbG8h".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), "Hello!".as_bytes());
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_empty_string() {
        let function = BlobB64url::new();

        let b64url_data = vec!["".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[] as &[u8]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_url_safe_characters() {
        let function = BlobB64url::new();

        // Base64url uses - and _ instead of + and /
        // This string contains URL-safe characters
        let b64url_data = vec!["SGVsbG9fV29ybGQtSGVsbG8".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), "Hello_World-Hello".as_bytes());
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_no_padding() {
        let function = BlobB64url::new();

        // Base64url typically omits padding characters
        // "Hello" in base64url without padding is "SGVsbG8"
        let b64url_data = vec!["SGVsbG8".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), "Hello".as_bytes());
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_multiple_rows() {
        let function = BlobB64url::new();

        // "A" = "QQ", "BC" = "QkM", "DEF" = "REVG" (no padding in base64url)
        let b64url_data = vec!["QQ".to_string(), "QkM".to_string(), "REVG".to_string()];
        let bitvec = vec![true, true, true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), "A".as_bytes());
            assert_eq!(container[1].as_bytes(), "BC".as_bytes());
            assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_with_null_data() {
        let function = BlobB64url::new();

        let b64url_data = vec!["QQ".to_string(), "".to_string(), "REVG".to_string()];
        let bitvec = vec![true, false, true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 3).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 3);
            assert!(container.is_defined(0));
            assert!(!container.is_defined(1));
            assert!(container.is_defined(2));

            assert_eq!(container[0].as_bytes(), "A".as_bytes());
            assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
            assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_binary_data() {
        let function = BlobB64url::new();

        // Binary data: [0xde, 0xad, 0xbe, 0xef] in base64url is "3q2-7w" (no padding)
        let b64url_data = vec!["3q2-7w".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1).unwrap();

        if let EngineColumnData::Blob(container) = result {
            assert_eq!(container.len(), 1);
            assert!(container.is_defined(0));
            assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
        } else {
            panic!("Expected BLOB column data");
        }
    }

    #[test]
    fn test_blob_b64url_invalid_input_should_error() {
        let function = BlobB64url::new();

        // Using standard base64 characters that are invalid in base64url
        let b64url_data = vec!["invalid+base64/chars".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for invalid base64url input");
    }

    #[test]
    fn test_blob_b64url_with_standard_base64_padding_should_error() {
        let function = BlobB64url::new();

        // Base64url typically doesn't use padding, so this should error
        let b64url_data = vec!["SGVsbG8=".to_string()];
        let bitvec = vec![true];
        let input_column = EngineColumn::ColumnQualified(ColumnQualified {
            name: "input".to_string(),
            data: EngineColumnData::Utf8(StringContainer::new(b64url_data, bitvec.into())),
        });

        let result = function.scalar(&[input_column], 1);
        assert!(result.is_err(), "Expected error for base64url with padding characters");
    }
}
