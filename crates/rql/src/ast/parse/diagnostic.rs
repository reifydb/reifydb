// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::parse::Error;
use crate::ast::parse::Error::{InvalidType, UnexpectedToken, UnsupportedToken};
use Error::{InvalidPolicy, UnexpectedEndOfFile};
use reifydb_core::Diagnostic;

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            UnexpectedToken { expected, got } => Diagnostic {
                code: "PA_001".to_string(),
                message: format!("unexpected token: expected `{}`", expected),
                span: Some(got.span.clone()),
                label: Some(format!("found `{}`", got.span.fragment)),
                help: Some(format!("expected token of kind `{}`", expected)),
                column: None,
                notes: vec![],
            },
            UnsupportedToken { got } => Diagnostic {
                code: "PA_002".to_string(),
                message: format!("unsupported token `{}`", got.span.fragment),
                span: Some(got.span.clone()),
                label: Some("this token is not allowed here".to_string()),
                help: Some("check for misplaced symbols or keywords".to_string()),
                column: None,
                notes: vec![],
            },
            InvalidType { got } => Diagnostic {
                code: "PA_003".to_string(),
                message: format!("invalid type name: `{}`", got.span.fragment),
                span: Some(got.span.clone()),
                label: Some("not a recognized type".to_string()),
                help: None,
                column: None,
                notes: vec![],
            },
            InvalidPolicy { got } => Diagnostic {
                code: "PA_004".to_string(),
                message: format!("invalid policy: `{}`", got.span.fragment),
                span: Some(got.span.clone()),
                label: Some("not a recognized policy".to_string()),
                help: Some(
                    "expected one of: `saturation`, `underflow`, `default`, `not undefined`"
                        .to_string(),
                ),
                column: None,
                notes: vec![],
            },
            UnexpectedEndOfFile => Diagnostic {
                code: "PA_999".to_string(),
                message: "unexpected end of input".to_string(),
                span: None,
                label: None,
                help: Some("did you forget to complete your expression?".to_string()),
                column: None,
                notes: vec![],
            },
        }
    }
}
