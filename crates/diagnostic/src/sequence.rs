// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Diagnostic;
use crate::util::value_max;
use reifydb_core::DataType;

pub fn sequence_exhausted(value: DataType) -> Diagnostic {
    Diagnostic {
        code: "SQ_001".to_string(),
        statement: None,
        message: format!("sequence generator of type `{}` is exhausted", value),
        span: None,
        label: Some("no more values can be generated".to_string()),
        help: Some(format!("maximum value for `{}` is `{}`", value, value_max(DataType::Uint4))),
        column: None,
        notes: vec![],
    }
}
