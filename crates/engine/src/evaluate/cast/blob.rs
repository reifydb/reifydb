// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::frame::ColumnValues;
use reifydb_core::error::diagnostic::cast;
use reifydb_core::value::Blob;
use reifydb_core::{OwnedSpan, Type, err, Error};

pub fn to_blob(values: &ColumnValues, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
    match values {
        ColumnValues::Utf8(strings, bitvec) => {
            let blobs: Vec<Blob> = strings.iter()
                .map(|s| Blob::from_utf8(OwnedSpan::testing(s)))
                .collect();
            Ok(ColumnValues::blob_with_bitvec(blobs, bitvec.clone()))
        }
        ColumnValues::Blob(_, _) => Ok(values.clone()),
        _ => {
            let source_type = values.get_type();
            err!(cast::unsupported_cast(span(), source_type, Type::Blob))
        }
    }
}

pub fn from_blob(values: &ColumnValues, target: Type, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
    match (values, target) {
        (ColumnValues::Blob(blobs, bitvec), Type::Utf8) => {
            let mut strings = Vec::with_capacity(blobs.len());
            for (idx, blob) in blobs.iter().enumerate() {
                if bitvec.get(idx) {
                    match blob.to_utf8() {
                        Ok(s) => strings.push(s),
                        Err(e) => return Err(Error(
                            cast::invalid_blob_to_utf8(span(), e.diagnostic())
                        )),
                    }
                } else {
                    strings.push(String::new()); // placeholder for undefined
                }
            }
            Ok(ColumnValues::utf8_with_bitvec(strings, bitvec.clone()))
        }
        (ColumnValues::Blob(_, _), _) => {
            err!(cast::unsupported_cast(span(), Type::Blob, target))
        }
        _ => unreachable!("from_blob called with non-BLOB values"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::BitVec;

    #[test]
    fn test_utf8_to_blob() {
        let strings = vec!["Hello".to_string(), "World".to_string()];
        let bitvec = BitVec::new(2, true);
        let values = ColumnValues::utf8_with_bitvec(strings, bitvec);
        
        let result = to_blob(&values, || OwnedSpan::testing_empty()).unwrap();
        
        match result {
            ColumnValues::Blob(blobs, _) => {
                assert_eq!(blobs[0].as_bytes(), b"Hello");
                assert_eq!(blobs[1].as_bytes(), b"World");
            }
            _ => panic!("Expected BLOB column values"),
        }
    }

    #[test]
    fn test_blob_to_utf8() {
        let blobs = vec![
            Blob::from_utf8(OwnedSpan::testing("Hello")),
            Blob::from_utf8(OwnedSpan::testing("World")),
        ];
        let bitvec = BitVec::new(2, true);
        let values = ColumnValues::blob_with_bitvec(blobs, bitvec);
        
        let result = from_blob(&values, Type::Utf8, || OwnedSpan::testing_empty()).unwrap();
        
        match result {
            ColumnValues::Utf8(strings, _) => {
                assert_eq!(strings[0], "Hello");
                assert_eq!(strings[1], "World");
            }
            _ => panic!("Expected UTF8 column values"),
        }
    }

    #[test]
    fn test_blob_to_utf8_invalid() {
        let blobs = vec![
            Blob::new(vec![0xFF, 0xFE]), // Invalid UTF-8
        ];
        let bitvec = BitVec::new(1, true);
        let values = ColumnValues::blob_with_bitvec(blobs, bitvec);
        
        let result = from_blob(&values, Type::Utf8, || OwnedSpan::testing_empty());
        assert!(result.is_err());
    }

    #[test]
    fn test_blob_identity_cast() {
        let blobs = vec![
            Blob::from_utf8(OwnedSpan::testing("test")),
        ];
        let bitvec = BitVec::new(1, true);
        let values = ColumnValues::blob_with_bitvec(blobs, bitvec.clone());
        
        let result = to_blob(&values, || OwnedSpan::testing_empty()).unwrap();
        
        match result {
            ColumnValues::Blob(result_blobs, result_bitvec) => {
                assert_eq!(result_blobs[0].as_bytes(), b"test");
                assert_eq!(result_bitvec, bitvec);
            }
            _ => panic!("Expected BLOB column values"),
        }
    }

    #[test]
    fn test_unsupported_to_blob_cast() {
        let ints = vec![42i32];
        let bitvec = BitVec::new(1, true);
        let values = ColumnValues::int4_with_bitvec(ints, bitvec);
        
        let result = to_blob(&values, || OwnedSpan::testing_empty());
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_from_blob_cast() {
        let blobs = vec![
            Blob::from_utf8(OwnedSpan::testing("test")),
        ];
        let bitvec = BitVec::new(1, true);
        let values = ColumnValues::blob_with_bitvec(blobs, bitvec);
        
        let result = from_blob(&values, Type::Int4, || OwnedSpan::testing_empty());
        assert!(result.is_err());
    }
}