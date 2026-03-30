// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{constraint::TypeConstraint, sumtype::VariantRef};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, ProcedureId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum ProcedureTrigger {
	/// Invoked explicitly via CALL
	#[default]
	Call,
	/// Triggered by DISPATCH on an event variant
	Event {
		variant: VariantRef,
	},
	/// Invoked via CALL but dispatched to a registered native (Rust) implementation
	NativeCall {
		native_name: String,
	},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Procedure {
	pub id: ProcedureId,
	pub namespace: NamespaceId,
	pub name: String,
	pub params: Vec<ProcedureParam>,
	pub return_type: Option<TypeConstraint>,
	/// RQL source text, compiled on load
	pub body: String,
	pub trigger: ProcedureTrigger,
	/// Test procedures can only be called from test context
	pub is_test: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureParam {
	pub name: String,
	pub param_type: TypeConstraint,
}
