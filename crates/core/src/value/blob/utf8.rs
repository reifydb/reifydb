// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::Blob;
use crate::error::diagnostic::blob;
use crate::{Error, Span};

impl Blob {
    pub fn from_utf8(span: impl Span) -> Self {
        let utf8_str = span.fragment();
        Blob::new(utf8_str.as_bytes().to_vec())
    }

    pub fn to_utf8(&self) -> Result<String, Error> {
        match std::str::from_utf8(self.as_bytes()) {
            Ok(s) => Ok(s.to_string()),
            Err(e) => Err(Error(blob::invalid_utf8_sequence(e))),
        }
    }

    pub fn to_utf8_lossy(&self) -> String {
        String::from_utf8_lossy(self.as_bytes()).to_string()
    }

    pub fn from_str(span: impl Span) -> Self {
        Self::from_utf8(span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedSpan;

    #[test]
    fn test_from_utf8() {
        let blob = Blob::from_utf8(OwnedSpan::testing("Hello, World!"));
        assert_eq!(blob.as_bytes(), b"Hello, World!");
    }

    #[test]
    fn test_from_utf8_unicode() {
        let blob = Blob::from_utf8(OwnedSpan::testing("Hello, 世界! 🦀"));
        assert_eq!(blob.as_bytes(), "Hello, 世界! 🦀".as_bytes());
    }

    #[test]
    fn test_from_utf8_empty() {
        let blob = Blob::from_utf8(OwnedSpan::testing(""));
        assert_eq!(blob.as_bytes(), b"");
    }

    #[test]
    fn test_to_utf8() {
        let blob = Blob::new("Hello, 世界!".as_bytes().to_vec());
        assert_eq!(blob.to_utf8().unwrap(), "Hello, 世界!");
    }

    #[test]
    fn test_to_utf8_invalid() {
        let blob = Blob::new(vec![0xFF, 0xFE]);
        assert!(blob.to_utf8().is_err());
    }

    #[test]
    fn test_to_utf8_lossy() {
        let blob = Blob::new("Hello, 世界!".as_bytes().to_vec());
        assert_eq!(blob.to_utf8_lossy(), "Hello, 世界!");

        let invalid_blob = Blob::new(vec![0xFF, 0xFE]);
        let lossy = invalid_blob.to_utf8_lossy();
        assert!(lossy.contains('�')); // replacement character
    }

    #[test]
    fn test_from_str() {
        let blob = Blob::from_str(OwnedSpan::testing("Hello!"));
        assert_eq!(blob.as_bytes(), b"Hello!");
    }

    #[test]
    fn test_utf8_roundtrip() {
        let original = "Hello, 世界! 🦀 Test with emojis and unicode";
        let blob = Blob::from_utf8(OwnedSpan::testing(original));
        let decoded = blob.to_utf8().unwrap();
        assert_eq!(decoded, original);
    }
}
