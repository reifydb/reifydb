// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::util::value_range;
use crate::{Diagnostic, DiagnosticColumn, Span};
use reifydb_core::ValueKind;

pub struct ColumnSaturation {
    pub span: Span,
    pub column: String,
    pub value: ValueKind,
}

pub fn column_saturation<'a>(co: ColumnSaturation) -> Diagnostic {
    let label = Some(format!(
        "value `{}` does not fit into `{}` (range: {})",
        &co.span.fragment,
        co.value,
        value_range(co.value)
    ));

    Diagnostic {
        code: "PO0001".to_string(),
        message: format!("value saturation in column `{}` type `{}`", co.column, co.value),
        span: Some(co.span),
        label,
        help: Some("reduce the value, change the column type to a wider type or change the saturation policy".to_string()),
        notes: vec![],
        column: Some(DiagnosticColumn { name: co.column, value: co.value }),
    }
}
