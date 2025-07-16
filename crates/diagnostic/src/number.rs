// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::util::value_range;
use crate::{Diagnostic, Span};
use reifydb_core::DataType;

pub fn invalid_number_format(span: Span, target: DataType) -> Diagnostic {
    let label = Some(format!("'{}' is not a valid {} number", span.fragment, target));

    let (help, notes) = match target {
        DataType::Float4 | DataType::Float8 => (
            "use decimal format (e.g., 123.45, -67.89, 1.23e-4)".to_string(),
            vec![
                "valid: 123.45".to_string(),
                "valid: -67.89".to_string(),
                "valid: 1.23e-4".to_string(),
            ],
        ),
        DataType::Int1
        | DataType::Int2
        | DataType::Int4
        | DataType::Int8
        | DataType::Int16
        | DataType::Uint1
        | DataType::Uint2
        | DataType::Uint4
        | DataType::Uint8
        | DataType::Uint16 => (
            "use integer format (e.g., 123, -456) or decimal that can be truncated".to_string(),
            vec![
                "valid: 123".to_string(),
                "valid: -456".to_string(),
                "truncated: 123.7 → 123".to_string(),
            ],
        ),
        _ => (
            "ensure the value is a valid number".to_string(),
            vec!["use a proper number format".to_string()],
        ),
    };

    Diagnostic {
        code: "NUMBER_001".to_string(),
        statement: None,
        message: "invalid number format".to_string(),
        span: Some(span),
        label,
        help: Some(help),
        notes,
        column: None,
        caused_by: None,
    }
}

pub fn number_out_of_range(span: Span, target: DataType) -> Diagnostic {
    let range = value_range(target);
    let label = Some(format!(
        "value '{}' exceeds the valid range for type {} ({})",
        span.fragment, target, range
    ));

    Diagnostic {
        code: "NUMBER_002".to_string(),
        statement: None,
        message: "number out of range".to_string(),
        span: Some(span),
        label,
        help: Some(format!(
            "use a value within the valid range for {} or use a wider type",
            target
        )),
        notes: vec![format!("valid range: {}", range)],
        column: None,
        caused_by: None,
    }
}
