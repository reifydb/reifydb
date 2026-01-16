// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Diagnostic conversion for parser errors.

use reifydb_type::{
	error::diagnostic::{Diagnostic, IntoDiagnostic},
	fragment::Fragment,
};

use crate::ast::parse::error::{ParseError, ParseErrorKind};

/// Wrapper for Vec<ParseError> to implement IntoDiagnostic.
pub struct ParseErrors(pub Vec<ParseError>);

impl From<Vec<ParseError>> for ParseErrors {
	fn from(errors: Vec<ParseError>) -> Self {
		ParseErrors(errors)
	}
}

impl IntoDiagnostic for ParseErrors {
	fn into_diagnostic(self) -> Diagnostic {
		let errors = self.0;
		if errors.is_empty() {
			return Diagnostic {
				code: "PARSE_000".to_string(),
				statement: None,
				message: "unknown parse error".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			};
		}

		// Use the first error as the primary diagnostic
		let first = &errors[0];
		let mut diagnostic = parse_error_to_diagnostic_single(first);

		// Add additional errors as notes
		if errors.len() > 1 {
			diagnostic.notes.push(format!("and {} more error(s)", errors.len() - 1));
			for error in errors.iter().skip(1).take(3) {
				diagnostic.notes.push(format!(
					"  at {}:{} - {}",
					error.span.line, error.span.column, error.kind
				));
			}
		}

		diagnostic
	}
}

fn parse_error_to_diagnostic_single(err: &ParseError) -> Diagnostic {
	let span = err.span;
	let fragment = Fragment::statement("", span.line, span.column);

	match &err.kind {
		ParseErrorKind::ExpectedKeyword(kw) => Diagnostic {
			code: "PARSE_001".to_string(),
			statement: None,
			message: format!("expected keyword '{:?}'", kw),
			column: None,
			fragment,
			label: Some("expected keyword here".to_string()),
			help: Some(format!("use the '{:?}' keyword", kw)),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::ExpectedOperator(op) => Diagnostic {
			code: "PARSE_002".to_string(),
			statement: None,
			message: format!("expected operator '{:?}'", op),
			column: None,
			fragment,
			label: Some("expected operator here".to_string()),
			help: Some(format!("use the '{:?}' operator", op)),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::ExpectedPunctuation(p) => Diagnostic {
			code: "PARSE_003".to_string(),
			statement: None,
			message: format!("expected punctuation '{:?}'", p),
			column: None,
			fragment,
			label: Some("expected punctuation here".to_string()),
			help: Some(format!("add '{:?}'", p)),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::ExpectedIdentifier => Diagnostic {
			code: "PARSE_004".to_string(),
			statement: None,
			message: "expected identifier".to_string(),
			column: None,
			fragment,
			label: Some("identifier expected".to_string()),
			help: Some("provide a valid identifier".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::ExpectedVariable => Diagnostic {
			code: "PARSE_005".to_string(),
			statement: None,
			message: "expected variable".to_string(),
			column: None,
			fragment,
			label: Some("variable expected".to_string()),
			help: Some("use a variable name starting with $".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::ExpectedExpression => Diagnostic {
			code: "PARSE_006".to_string(),
			statement: None,
			message: "expected expression".to_string(),
			column: None,
			fragment,
			label: Some("expression expected".to_string()),
			help: Some("provide a valid expression".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::UnexpectedToken => Diagnostic {
			code: "PARSE_007".to_string(),
			statement: None,
			message: "unexpected token".to_string(),
			column: None,
			fragment,
			label: Some("unexpected token".to_string()),
			help: Some("check syntax".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::NotImplemented(feature) => Diagnostic {
			code: "PARSE_008".to_string(),
			statement: None,
			message: format!("not implemented: {}", feature),
			column: None,
			fragment,
			label: Some("not yet supported".to_string()),
			help: Some("this feature is not yet implemented".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
		ParseErrorKind::Custom(msg) => Diagnostic {
			code: "PARSE_009".to_string(),
			statement: None,
			message: msg.clone(),
			column: None,
			fragment,
			label: None,
			help: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		},
	}
}
