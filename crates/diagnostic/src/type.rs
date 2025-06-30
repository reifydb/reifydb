// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::util::value_range;
use crate::{Diagnostic, Span};
use reifydb_core::{DiagnosticColumn, Kind};

pub struct OutOfRange {
    pub span: Span,
    pub column: Option<String>,
    pub kind: Option<Kind>,
}

pub fn out_of_range(co: OutOfRange) -> Diagnostic {
    let label = match (&co.kind, &co.column) {
        (Some(ty), Some(column)) => Some(format!(
            "value `{}` does not fit into column `{}` of type `{}` (range: {})",
            co.span.fragment,
            column,
            ty,
            value_range(*ty),
        )),
        (Some(ty), None) => Some(format!(
            "value `{}` does not fit into type `{}` (range: {})",
            co.span.fragment,
            ty,
            value_range(*ty)
        )),
        (None, Some(column)) => {
            Some(format!("value `{}` does not fit into column `{}`", co.span.fragment, column))
        }
        (None, None) => Some(format!("value `{}` does not fit into column type", co.span.fragment)),
    };

    let message = match (&co.column, &co.kind) {
        (Some(column), Some(value)) => {
            format!("value out of range in column `{}` type `{}`", column, value)
        }
        (Some(column), None) => format!("value out of range in column `{}`", column),
        (None, Some(value)) => format!("value out of range in type `{}`", value),
        (None, None) => "value out of range".to_string(),
    };

    let help = Some(
        "reduce the value, change the column type to a wider type or change the saturation policy"
            .to_string(),
    );

    Diagnostic {
        code: "TYPE_001".to_string(),
        message,
        span: Some(co.span),
        label,
        help,
        notes: vec![],
        column: match (&co.column, co.kind) {
            (Some(name), Some(value)) => Some(DiagnosticColumn { name: name.clone(), value }),
            _ => None,
        },
    }
}
