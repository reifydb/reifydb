// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Diagnostic conversion for planner errors.

use reifydb_type::{
	Fragment,
	diagnostic::{Diagnostic, IntoDiagnostic},
};

use crate::plan::compile::{PlanError, PlanErrorKind};

impl IntoDiagnostic for PlanError {
	fn into_diagnostic(self) -> Diagnostic {
		let span = self.span;
		let fragment = Fragment::statement("", span.line, span.column);

		match self.kind {
			PlanErrorKind::NamespaceNotFound(name) => Diagnostic {
				code: "PLAN_001".to_string(),
				statement: None,
				message: format!("namespace not found: {}", name),
				column: None,
				fragment,
				label: Some("namespace does not exist".to_string()),
				help: Some("check the namespace name or create it first".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::TableNotFound(name) => Diagnostic {
				code: "PLAN_002".to_string(),
				statement: None,
				message: format!("table not found: {}", name),
				column: None,
				fragment,
				label: Some("table does not exist".to_string()),
				help: Some("check the table name or create it first".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::ViewNotFound(name) => Diagnostic {
				code: "PLAN_003".to_string(),
				statement: None,
				message: format!("view not found: {}", name),
				column: None,
				fragment,
				label: Some("view does not exist".to_string()),
				help: Some("check the view name or create it first".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::ColumnNotFound(name) => Diagnostic {
				code: "PLAN_004".to_string(),
				statement: None,
				message: format!("column not found: {}", name),
				column: None,
				fragment,
				label: Some("column does not exist in the current context".to_string()),
				help: Some("check for typos or ensure the column is defined in the input".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::VariableNotFound(name) => Diagnostic {
				code: "PLAN_005".to_string(),
				statement: None,
				message: format!("variable not found: {}", name),
				column: None,
				fragment,
				label: Some("variable is not defined".to_string()),
				help: Some("declare the variable before using it".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::FunctionNotFound(name) => Diagnostic {
				code: "PLAN_006".to_string(),
				statement: None,
				message: format!("function not found: {}", name),
				column: None,
				fragment,
				label: Some("function does not exist".to_string()),
				help: Some("check the function name or ensure it is registered".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::EmptyPipeline => Diagnostic {
				code: "PLAN_007".to_string(),
				statement: None,
				message: "empty pipeline".to_string(),
				column: None,
				fragment,
				label: Some("pipeline has no operations".to_string()),
				help: Some("add at least one operation to the pipeline".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::MissingInput(op) => Diagnostic {
				code: "PLAN_008".to_string(),
				statement: None,
				message: format!("{} requires input", op),
				column: None,
				fragment,
				label: Some("missing input".to_string()),
				help: Some("ensure this operation has input from a previous operation".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::TypeMismatch {
				expected,
				found,
			} => Diagnostic {
				code: "PLAN_009".to_string(),
				statement: None,
				message: format!("type mismatch: expected {}, found {}", expected, found),
				column: None,
				fragment,
				label: Some("type mismatch".to_string()),
				help: Some(format!("convert to {} or use a different operation", expected)),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PlanErrorKind::Unsupported(msg) => Diagnostic {
				code: "PLAN_010".to_string(),
				statement: None,
				message: format!("unsupported: {}", msg),
				column: None,
				fragment,
				label: Some("not supported".to_string()),
				help: Some("use a different approach".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}
