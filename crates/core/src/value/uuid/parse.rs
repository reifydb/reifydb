// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::uuid::{invalid_uuid4_format, invalid_uuid7_format};
use crate::value::{Uuid4, Uuid7};
use crate::{Error, Span, err};
use ::uuid::Uuid;

pub fn parse_uuid4(span: impl Span) -> Result<Uuid4, Error> {
    let value = span.fragment().trim();

    if let Ok(uuid) = Uuid::parse_str(value) {
        if uuid.get_version_num() == 4 {
            return Ok(Uuid4(uuid));
        }
    }
    err!(invalid_uuid4_format(span.to_owned()))
}

pub fn parse_uuid7(span: impl Span) -> Result<Uuid7, Error> {
    let value = span.fragment().trim();
    if let Ok(uuid) = Uuid::parse_str(value) {
        if uuid.get_version_num() == 7 {
            return Ok(Uuid7(uuid));
        }
    }

    err!(invalid_uuid7_format(span.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedSpan;

    mod uuid4 {
        use super::*;

        #[test]
        fn test_valid_uuid4() {
            let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
            let result = parse_uuid4(OwnedSpan::testing(uuid_str));
            assert!(result.is_ok());
            let uuid = result.unwrap();
            assert_eq!(uuid.get_version_num(), 4);
        }

        #[test]
        fn test_valid_uuid4_uppercase() {
            let uuid_str = "550E8400-E29B-41D4-A716-446655440000";
            let result = parse_uuid4(OwnedSpan::testing(uuid_str));
            assert!(result.is_ok());
            let uuid = result.unwrap();
            assert_eq!(uuid.get_version_num(), 4);
        }

        #[test]
        fn test_valid_uuid4_with_spaces() {
            let uuid_str = "  550e8400-e29b-41d4-a716-446655440000  ";
            let result = parse_uuid4(OwnedSpan::testing(uuid_str));
            assert!(result.is_ok());
            let uuid = result.unwrap();
            assert_eq!(uuid.get_version_num(), 4);
        }

        #[test]
        fn test_invalid_uuid4_empty() {
            let result = parse_uuid4(OwnedSpan::testing(""));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid4_whitespace() {
            let result = parse_uuid4(OwnedSpan::testing("   "));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid4_format() {
            let result = parse_uuid4(OwnedSpan::testing("not-a-uuid"));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid4_wrong_version() {
            // This is a UUID v1, should fail validation for v4
            let uuid_str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
            let result = parse_uuid4(OwnedSpan::testing(uuid_str));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid4_malformed() {
            let result = parse_uuid4(OwnedSpan::testing("550e8400-e29b-41d4-a716"));
            assert!(result.is_err());
        }
    }

    mod uuid7 {
        use super::*;

        #[test]
        fn test_valid_uuid7() {
            let uuid_str = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
            let result = parse_uuid7(OwnedSpan::testing(uuid_str));
            assert!(result.is_ok());
            let uuid = result.unwrap();
            assert_eq!(uuid.get_version_num(), 7);
        }

        #[test]
        fn test_valid_uuid7_uppercase() {
            let uuid_str = "017F22E2-79B0-7CC3-98C4-DC0C0C07398F";
            let result = parse_uuid7(OwnedSpan::testing(uuid_str));
            assert!(result.is_ok());
            let uuid = result.unwrap();
            assert_eq!(uuid.get_version_num(), 7);
        }

        #[test]
        fn test_valid_uuid7_with_spaces() {
            let uuid_str = "  017f22e2-79b0-7cc3-98c4-dc0c0c07398f  ";
            let result = parse_uuid7(OwnedSpan::testing(uuid_str));
            assert!(result.is_ok());
            let uuid = result.unwrap();
            assert_eq!(uuid.get_version_num(), 7);
        }

        #[test]
        fn test_invalid_uuid7_empty() {
            let result = parse_uuid7(OwnedSpan::testing(""));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid7_whitespace() {
            let result = parse_uuid7(OwnedSpan::testing("   "));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid7_format() {
            let result = parse_uuid7(OwnedSpan::testing("invalid-uuid"));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid7_wrong_version() {
            // This is a UUID v4, should fail validation for v7
            let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
            let result = parse_uuid7(OwnedSpan::testing(uuid_str));
            assert!(result.is_err());
        }

        #[test]
        fn test_invalid_uuid7_malformed() {
            let result = parse_uuid7(OwnedSpan::testing("017f22e2-79b0-7cc3"));
            assert!(result.is_err());
        }
    }
}
