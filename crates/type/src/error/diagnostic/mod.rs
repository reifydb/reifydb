// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use ::serde::{Deserialize, Serialize};

use crate::{fragment::Fragment, value::r#type::Type};

pub mod ast;
pub mod auth;
pub mod blob;
pub mod boolean;
pub mod cast;
pub mod constraint;
pub mod conversion;
pub mod dictionary;
pub mod function;
pub mod network;
pub mod number;
pub mod operator;
pub mod render;
pub mod runtime;
pub mod serde;
pub mod temporal;
pub mod util;
pub mod uuid;

/// Entry in the operator call chain for flow operator errors
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorChainEntry {
	pub node_id: u64,
	pub operator_name: String,
	pub operator_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
	pub code: String,
	pub statement: Option<String>,
	pub message: String,
	pub column: Option<DiagnosticColumn>,
	pub fragment: Fragment,
	pub label: Option<String>,
	pub help: Option<String>,
	pub notes: Vec<String>,
	pub cause: Option<Box<Diagnostic>>,
	/// Operator call chain when error occurred (for flow operator errors)
	pub operator_chain: Option<Vec<OperatorChainEntry>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticColumn {
	pub name: String,
	pub r#type: Type,
}

impl Default for Diagnostic {
	fn default() -> Self {
		Self {
			code: String::new(),
			statement: None,
			message: String::new(),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: None,
			notes: Vec::new(),
			cause: None,
			operator_chain: None,
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
			let mut updated_cause = std::mem::replace(cause.as_mut(), Diagnostic::default());
			updated_cause.with_statement(statement);
			*cause = Box::new(updated_cause);
		}
	}

	/// Set or update the fragment for this diagnostic and all nested
	/// diagnostics recursively
	pub fn with_fragment(&mut self, new_fragment: Fragment) {
		// Always update the fragment, not just when it's None
		// This is needed for cast errors that need to update the
		// fragment
		self.fragment = new_fragment;

		if let Some(ref mut cause) = self.cause {
			cause.with_fragment(self.fragment.clone());
		}
	}

	/// Get the fragment if this is a Statement fragment (for backward
	/// compatibility)
	pub fn fragment(&self) -> Option<Fragment> {
		match &self.fragment {
			Fragment::Statement {
				..
			} => Some(self.fragment.clone()),
			_ => None,
		}
	}
}

/// Trait for converting error types into Diagnostic.
///
/// Implement this trait to provide rich diagnostic information for custom error types.
/// The trait consumes the error (takes `self` by value) to allow moving owned data
/// into the diagnostic.
pub trait IntoDiagnostic {
	/// Convert self into a Diagnostic with error code, message, fragment, and other metadata.
	fn into_diagnostic(self) -> Diagnostic;
}
