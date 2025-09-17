// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

pub mod ast;
pub mod auth;
pub mod blob;
pub mod boolean;
pub mod cast;
pub mod catalog;
pub mod constraint;
pub mod conversion;
pub mod engine;
pub mod flow;
pub mod function;
pub mod internal;
pub use internal::{internal, internal_with_context};

use crate::{OwnedFragment, Type, fragment::IntoFragment};

pub mod network;
pub mod number;
pub mod operation;
pub mod operator;
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
	pub fragment: OwnedFragment,
	pub label: Option<String>,
	pub help: Option<String>,
	pub notes: Vec<String>,
	pub cause: Option<Box<Diagnostic>>,
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
			fragment: OwnedFragment::None,
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
			let mut updated_cause = std::mem::replace(cause.as_mut(), Diagnostic::default());
			updated_cause.with_statement(statement);
			*cause = Box::new(updated_cause);
		}
	}

	/// Set or update the fragment for this diagnostic and all nested
	/// diagnostics recursively
	pub fn with_fragment<'a>(&mut self, new_fragment: impl IntoFragment<'a>) {
		// Always update the fragment, not just when it's None
		// This is needed for cast errors that need to update the
		// fragment
		self.fragment = new_fragment.into_fragment().into_owned();

		if let Some(ref mut cause) = self.cause {
			cause.with_fragment(self.fragment.clone());
		}
	}

	/// Get the fragment if this is a Statement fragment (for backward
	/// compatibility)
	pub fn fragment(&self) -> Option<OwnedFragment> {
		match &self.fragment {
			OwnedFragment::Statement {
				..
			} => Some(self.fragment.clone()),
			_ => None,
		}
	}
}
