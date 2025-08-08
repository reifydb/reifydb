// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::error::diagnostic::Diagnostic;
use crate::result::error::diagnostic::util::value_max;
use crate::{IntoOwnedSpan, Type};

pub fn sequence_exhausted(value: Type) -> Diagnostic {
    Diagnostic {
        code: "SEQUENCE_001".to_string(),
        statement: None,
        message: format!("sequence generator of type `{}` is exhausted", value),
        span: None,
        label: Some("no more values can be generated".to_string()),
        help: Some(format!("maximum value for `{}` is `{}`", value, value_max(value))),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn can_not_alter_not_auto_increment(span: impl IntoOwnedSpan) -> Diagnostic {
    let span = span.into_span();
    Diagnostic {
        code: "SEQUENCE_002".to_string(),
        statement: None,
        message: format!(
            "cannot alter sequence for column `{}` which does not have AUTO INCREMENT",
            &span
        ),
        span: Some(span),
        label: Some("column does not have AUTO INCREMENT".to_string()),
        help: Some("only columns with AUTO INCREMENT can have their sequences altered".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}
