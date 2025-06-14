// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Diagnostic;
use crate::util::value_max;
use reifydb_core::ValueKind;

impl Diagnostic {
    pub fn sequence_exhausted(value: ValueKind) -> Self {
        Diagnostic {
            code: "SQ001".to_string(),
            message: format!("sequence generator of type `{}` is exhausted", value),
            span: None,
            label: Some("no more values can be generated".to_string()),
            help: Some(format!(
                "maximum value for `{}` is `{}`",
                value,
                value_max(ValueKind::Uint4)
            )),
            column: None,
            notes: vec![],
        }
    }
}
