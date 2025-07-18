// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::boolean::{empty_boolean_value, invalid_boolean_format, invalid_numeric_boolean};

use crate::{Error, Span};

pub fn parse_bool(span: &Span) -> Result<bool, Error> {
    let value = span.fragment.trim();

    if value.is_empty() {
        return Err(Error(empty_boolean_value(span.clone())));
    }

    match value.to_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        "1" | "1.0" => Ok(true),
        "0" | "0.0" => Ok(false),
        _ => {
            // Check if the value contains numbers - if so, use numeric boolean diagnostic
            if value.chars().any(|c| c.is_ascii_digit()) {
                Err(Error(invalid_numeric_boolean(span.clone())))
            } else {
                Err(Error(invalid_boolean_format(span.clone())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;

    #[test]
    fn test_valid_true() {
        assert_eq!(parse_bool(&Span::testing("true")), Ok(true));
    }

    #[test]
    fn test_valid_false() {
        assert_eq!(parse_bool(&Span::testing("false")), Ok(false));
    }

    #[test]
    fn test_valid_true_with_spaces() {
        assert_eq!(parse_bool(&Span::testing("  true  ")), Ok(true));
    }

    #[test]
    fn test_valid_false_with_spaces() {
        assert_eq!(parse_bool(&Span::testing("  false  ")), Ok(false));
    }

    #[test]
    fn test_case_mismatch_true() {
        assert_eq!(parse_bool(&Span::testing("True")), Ok(true));
        assert_eq!(parse_bool(&Span::testing("TRUE")), Ok(true));
        assert_eq!(parse_bool(&Span::testing("tRuE")), Ok(true));
    }

    #[test]
    fn test_case_mismatch_false() {
        assert_eq!(parse_bool(&Span::testing("False")), Ok(false));
        assert_eq!(parse_bool(&Span::testing("FALSE")), Ok(false));
        assert_eq!(parse_bool(&Span::testing("fAlSe")), Ok(false));
    }

    #[test]
    fn test_valid_numeric_boolean() {
        assert_eq!(parse_bool(&Span::testing("1")), Ok(true));
        assert_eq!(parse_bool(&Span::testing("0")), Ok(false));
        assert_eq!(parse_bool(&Span::testing("1.0")), Ok(true));
        assert_eq!(parse_bool(&Span::testing("0.0")), Ok(false));
    }

    #[test]
    fn test_invalid_numeric_boolean() {
        assert!(parse_bool(&Span::testing("2")).is_err());
        assert!(parse_bool(&Span::testing("1.5")).is_err());
        assert!(parse_bool(&Span::testing("0.5")).is_err());
        assert!(parse_bool(&Span::testing("-1")).is_err());
        assert!(parse_bool(&Span::testing("100")).is_err());
    }

    #[test]
    fn test_empty_boolean_value() {
        assert!(parse_bool(&Span::testing("")).is_err());
        assert!(parse_bool(&Span::testing("   ")).is_err());
    }

    #[test]
    fn test_ambiguous_boolean_value() {
        assert!(parse_bool(&Span::testing("yes")).is_err());
        assert!(parse_bool(&Span::testing("no")).is_err());
        assert!(parse_bool(&Span::testing("y")).is_err());
        assert!(parse_bool(&Span::testing("n")).is_err());
        assert!(parse_bool(&Span::testing("on")).is_err());
        assert!(parse_bool(&Span::testing("off")).is_err());
        assert!(parse_bool(&Span::testing("t")).is_err());
        assert!(parse_bool(&Span::testing("f")).is_err());
    }

    #[test]
    fn test_invalid_boolean_format() {
        assert!(parse_bool(&Span::testing("invalid")).is_err());
        assert!(parse_bool(&Span::testing("123")).is_err());
        assert!(parse_bool(&Span::testing("abc")).is_err());
        assert!(parse_bool(&Span::testing("maybe")).is_err());
    }

    #[test]
    fn test_case_insensitive_ambiguous() {
        assert!(parse_bool(&Span::testing("Yes")).is_err());
        assert!(parse_bool(&Span::testing("NO")).is_err());
        assert!(parse_bool(&Span::testing("On")).is_err());
        assert!(parse_bool(&Span::testing("OFF")).is_err());
    }
}
