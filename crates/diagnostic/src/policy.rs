// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::util::value_range;
use crate::{Diagnostic, DiagnosticColumn, Span};
use reifydb_core::ValueKind;

pub struct ColumnSaturation {
    pub span: Span,
    pub column: Option<String>,
    pub value: Option<ValueKind>,
}

pub fn column_saturation(co: ColumnSaturation) -> Diagnostic {
    let range = co.value.map(value_range);

    let label = match (&co.value, &co.column) {
        (Some(value), Some(column)) => Some(format!(
            "value `{}` does not fit into `{}` (range: {})",
            co.span.fragment,
            value,
            value_range(*value)
        )),
        (Some(value), None) => Some(format!(
            "value `{}` does not fit into type `{}` (range: {})",
            co.span.fragment,
            value,
            value_range(*value)
        )),
        (None, Some(column)) => {
            Some(format!("value `{}` does not fit into column `{}`", co.span.fragment, column))
        }
        (None, None) => Some(format!("value `{}` does not fit into column type", co.span.fragment)),
    };

    let message = match (&co.column, &co.value) {
        (Some(column), Some(value)) => {
            format!("value saturation in column `{}` type `{}`", column, value)
        }
        (Some(column), None) => format!("value saturation in column `{}`", column),
        (None, Some(value)) => format!("value saturation in type `{}`", value),
        (None, None) => "value saturation".to_string(),
    };

    let help = Some(
        "reduce the value, change the column type to a wider type or change the saturation policy"
            .to_string(),
    );

    Diagnostic {
        code: "PO_001".to_string(),
        message,
        span: Some(co.span),
        label,
        help,
        notes: vec![],
        column: match (&co.column, co.value) {
            (Some(name), Some(value)) => Some(DiagnosticColumn { name: name.clone(), value }),
            _ => None,
        },
    }
}
