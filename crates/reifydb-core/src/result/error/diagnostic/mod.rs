// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use self::origin::{DiagnosticOrigin, OwnedSpan};
use crate::{diagnostic_origin, Type};

pub mod ast;
pub mod auth;
pub mod blob;
pub mod boolean;
pub mod cast;
pub mod catalog;
pub mod conversion;
pub mod engine;
pub mod flow;
pub mod function;
pub mod network;
pub mod number;
pub mod operator;
pub mod origin;
pub mod query;
pub mod render;
pub mod sequence;
pub mod serialization;
pub mod temporal;
pub mod transaction;
mod util;
pub mod uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
	pub code: String,
	pub statement: Option<String>,
	pub message: String,
	pub column: Option<DiagnosticColumn>,

	pub origin: DiagnosticOrigin,
	pub label: Option<String>,
	pub help: Option<String>,
	pub notes: Vec<String>,
	pub cause: Option<Box<Diagnostic>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticColumn {
	pub name: String,
	pub ty: Type,
}

impl Default for Diagnostic {
	fn default() -> Self {
		Self {
			code: String::new(),
			statement: None,
			message: String::new(),
			column: None,
			origin: DiagnosticOrigin::None,
			label: None,
			help: None,
			notes: Vec::new(),
			cause: None,
		}
	}
}

impl Display for Diagnostic {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.write_fmt(format_args!("{}", self.code))
	}
}

impl Diagnostic {
	/// Set the statement for this diagnostic and all nested diagnostics
	/// recursively
	pub fn with_statement(&mut self, statement: String) {
		self.statement = Some(statement.clone());

		// Recursively set statement for all nested diagnostics
		if let Some(ref mut cause) = self.cause {
			let mut updated_cause = std::mem::replace(
				cause.as_mut(),
				Diagnostic::default(),
			);
			updated_cause.with_statement(statement);
			*cause = Box::new(updated_cause);
		}
	}

	/// Set or update the origin for this diagnostic and all nested
	/// diagnostics recursively
	pub fn with_origin(&mut self, new_origin: DiagnosticOrigin) {
		// Always update the origin, not just when it's None
		// This is needed for cast errors that need to update the span
		self.origin = new_origin.clone();
		
		if let Some(ref mut cause) = self.cause {
			cause.with_origin(new_origin);
		}
	}

	/// Compatibility method - converts span to DiagnosticOrigin
	/// Set or update the span for this diagnostic and all nested
	/// diagnostics recursively
	pub fn with_span(&mut self, new_span: &OwnedSpan) {
		// Use the macro to capture location where with_span was called
		self.with_origin(
			diagnostic_origin!(statement: new_span.clone()),
		);
	}

	/// Get the span if this is a Statement origin (for backward compatibility)
	pub fn span(&self) -> Option<OwnedSpan> {
		match &self.origin {
			DiagnosticOrigin::Statement {
				line,
				column,
				fragment,
				..
			} => Some(OwnedSpan {
				line: *line,
				column: *column,
				fragment: fragment.clone(),
			}),
			_ => None,
		}
	}
}
