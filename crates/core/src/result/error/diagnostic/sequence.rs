// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;
use crate::result::error::diagnostic::Diagnostic;
use crate::result::error::diagnostic::util::value_max;

pub fn sequence_exhausted(value: Type) -> Diagnostic {
    Diagnostic {
        code: "SQ_001".to_string(),
        statement: None,
        message: format!("sequence generator of type `{}` is exhausted", value),
        span: None,
        label: Some("no more values can be generated".to_string()),
        help: Some(format!("maximum value for `{}` is `{}`", value, value_max(Type::Uint4))),
        column: None,
        notes: vec![],
        cause: None,
    }
}
