// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::boolean::{
    empty_boolean_value, invalid_boolean_format, invalid_number_boolean,
};

use crate::{err, Error, Span, return_error};

pub fn parse_bool(span: impl Span) -> Result<bool, Error> {
    let value = span.fragment().trim();

    if value.is_empty() {
        return_error!(empty_boolean_value(span.to_owned()));
    }

    match value.to_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        "1" | "1.0" => Ok(true),
        "0" | "0.0" => Ok(false),
        _ => {
            // Check if the value contains numbers - if so, use numeric boolean diagnostic
            if value.chars().any(|c| c.is_ascii_digit()) {
                err!(invalid_number_boolean(span.to_owned()))
            } else {
                err!(invalid_boolean_format(span.to_owned()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedSpan;

    #[test]
    fn test_valid_true() {
        assert_eq!(parse_bool(OwnedSpan::testing("true")), Ok(true));
    }

    #[test]
    fn test_valid_false() {
        assert_eq!(parse_bool(OwnedSpan::testing("false")), Ok(false));
    }

    #[test]
    fn test_valid_true_with_spaces() {
        assert_eq!(parse_bool(OwnedSpan::testing("  true  ")), Ok(true));
    }

    #[test]
    fn test_valid_false_with_spaces() {
        assert_eq!(parse_bool(OwnedSpan::testing("  false  ")), Ok(false));
    }

    #[test]
    fn test_case_mismatch_true() {
        assert_eq!(parse_bool(OwnedSpan::testing("True")), Ok(true));
        assert_eq!(parse_bool(OwnedSpan::testing("TRUE")), Ok(true));
        assert_eq!(parse_bool(OwnedSpan::testing("tRuE")), Ok(true));
    }

    #[test]
    fn test_case_mismatch_false() {
        assert_eq!(parse_bool(OwnedSpan::testing("False")), Ok(false));
        assert_eq!(parse_bool(OwnedSpan::testing("FALSE")), Ok(false));
        assert_eq!(parse_bool(OwnedSpan::testing("fAlSe")), Ok(false));
    }

    #[test]
    fn test_valid_numeric_boolean() {
        assert_eq!(parse_bool(OwnedSpan::testing("1")), Ok(true));
        assert_eq!(parse_bool(OwnedSpan::testing("0")), Ok(false));
        assert_eq!(parse_bool(OwnedSpan::testing("1.0")), Ok(true));
        assert_eq!(parse_bool(OwnedSpan::testing("0.0")), Ok(false));
    }

    #[test]
    fn test_invalid_numeric_boolean() {
        assert!(parse_bool(OwnedSpan::testing("2")).is_err());
        assert!(parse_bool(OwnedSpan::testing("1.5")).is_err());
        assert!(parse_bool(OwnedSpan::testing("0.5")).is_err());
        assert!(parse_bool(OwnedSpan::testing("-1")).is_err());
        assert!(parse_bool(OwnedSpan::testing("100")).is_err());
    }

    #[test]
    fn test_empty_boolean_value() {
        assert!(parse_bool(OwnedSpan::testing("")).is_err());
        assert!(parse_bool(OwnedSpan::testing("   ")).is_err());
    }

    #[test]
    fn test_ambiguous_boolean_value() {
        assert!(parse_bool(OwnedSpan::testing("yes")).is_err());
        assert!(parse_bool(OwnedSpan::testing("no")).is_err());
        assert!(parse_bool(OwnedSpan::testing("y")).is_err());
        assert!(parse_bool(OwnedSpan::testing("n")).is_err());
        assert!(parse_bool(OwnedSpan::testing("on")).is_err());
        assert!(parse_bool(OwnedSpan::testing("off")).is_err());
        assert!(parse_bool(OwnedSpan::testing("t")).is_err());
        assert!(parse_bool(OwnedSpan::testing("f")).is_err());
    }

    #[test]
    fn test_invalid_boolean_format() {
        assert!(parse_bool(OwnedSpan::testing("invalid")).is_err());
        assert!(parse_bool(OwnedSpan::testing("123")).is_err());
        assert!(parse_bool(OwnedSpan::testing("abc")).is_err());
        assert!(parse_bool(OwnedSpan::testing("maybe")).is_err());
    }

    #[test]
    fn test_case_insensitive_ambiguous() {
        assert!(parse_bool(OwnedSpan::testing("Yes")).is_err());
        assert!(parse_bool(OwnedSpan::testing("NO")).is_err());
        assert!(parse_bool(OwnedSpan::testing("On")).is_err());
        assert!(parse_bool(OwnedSpan::testing("OFF")).is_err());
    }
}
