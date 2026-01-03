// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Core planner types and entry point.

use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_catalog::Catalog;
use reifydb_transaction::IntoStandardTransaction;
use tracing::instrument;

use super::scope::Scope;
use crate::{ast::Program, plan::Plan, token::Span};

/// Planner context - holds bump allocator, catalog, and transaction.
pub(super) struct Planner<'bump, 'cat, T> {
	pub(super) bump: &'bump Bump,
	pub(super) catalog: &'cat Catalog,
	pub(super) tx: &'cat mut T,
	pub(super) scopes: BumpVec<'bump, Scope<'bump>>,
	pub(super) next_variable_id: u32,
	/// Script function names that have been defined.
	pub(super) script_functions: BumpVec<'bump, &'bump str>,
}

/// Result type for plan compilation.
pub type Result<T> = std::result::Result<T, PlanError>;

/// Plan compilation error.
#[derive(Debug)]
pub struct PlanError {
	pub kind: PlanErrorKind,
	pub span: Span,
}

/// Kind of plan compilation error.
#[derive(Debug)]
pub enum PlanErrorKind {
	NamespaceNotFound(String),
	TableNotFound(String),
	ViewNotFound(String),
	ColumnNotFound(String),
	VariableNotFound(String),
	FunctionNotFound(String),
	EmptyPipeline,
	MissingInput(&'static str),
	TypeMismatch {
		expected: String,
		found: String,
	},
	Unsupported(String),
}

impl std::fmt::Display for PlanError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self.kind {
			PlanErrorKind::NamespaceNotFound(name) => write!(f, "namespace not found: {}", name),
			PlanErrorKind::TableNotFound(name) => write!(f, "table not found: {}", name),
			PlanErrorKind::ViewNotFound(name) => write!(f, "view not found: {}", name),
			PlanErrorKind::ColumnNotFound(name) => write!(f, "column not found: {}", name),
			PlanErrorKind::VariableNotFound(name) => write!(f, "variable not found: {}", name),
			PlanErrorKind::FunctionNotFound(name) => write!(f, "function not found: {}", name),
			PlanErrorKind::EmptyPipeline => write!(f, "empty pipeline"),
			PlanErrorKind::MissingInput(op) => write!(f, "{} requires input", op),
			PlanErrorKind::TypeMismatch {
				expected,
				found,
			} => {
				write!(f, "type mismatch: expected {}, found {}", expected, found)
			}
			PlanErrorKind::Unsupported(msg) => write!(f, "unsupported: {}", msg),
		}
	}
}

impl std::error::Error for PlanError {}

/// Default namespace name when none is specified.
pub(super) const DEFAULT_NAMESPACE: &str = "default";

/// Compile a program to a plan.
#[instrument(name = "rql::plan", level = "trace", skip(bump, catalog, tx, program))]
pub async fn plan<'bump, T: IntoStandardTransaction>(
	bump: &'bump Bump,
	catalog: &Catalog,
	tx: &mut T,
	program: Program<'bump>,
) -> Result<&'bump [Plan<'bump>]> {
	let mut planner = Planner {
		bump,
		catalog,
		tx,
		scopes: BumpVec::new_in(bump),
		next_variable_id: 0,
		script_functions: BumpVec::new_in(bump),
	};
	planner.push_scope();
	planner.compile_program(program).await
}
