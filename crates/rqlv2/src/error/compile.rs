// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Diagnostic conversion for bytecode compiler errors.

use reifydb_type::{
	Fragment,
	diagnostic::{Diagnostic, IntoDiagnostic},
};

use crate::bytecode::compile::CompileError;

impl IntoDiagnostic for CompileError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			CompileError::UnsupportedPlan {
				message,
				span,
			} => {
				let fragment = Fragment::statement("", span.line, span.column);
				Diagnostic {
					code: "COMPILE_001".to_string(),
					statement: None,
					message: format!("unsupported plan: {}", message),
					column: None,
					fragment,
					label: Some("cannot compile this plan".to_string()),
					help: Some("this plan node is not yet supported in bytecode compilation"
						.to_string()),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			CompileError::UnsupportedExpr {
				message,
				span,
			} => {
				let fragment = Fragment::statement("", span.line, span.column);
				Diagnostic {
					code: "COMPILE_002".to_string(),
					statement: None,
					message: format!("unsupported expression: {}", message),
					column: None,
					fragment,
					label: Some("cannot compile this expression".to_string()),
					help: Some("this expression is not yet supported in bytecode compilation"
						.to_string()),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
			CompileError::Internal {
				message,
			} => Diagnostic {
				code: "COMPILE_003".to_string(),
				statement: None,
				message: format!("internal compiler error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: Some("internal error".to_string()),
				help: Some("this is a bug in the compiler; please report it".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
