// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{constraint::TypeConstraint, sumtype::SumTypeId};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, ProcedureId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProcedureTrigger {
	/// Invoked explicitly via CALL
	Call,
	/// Triggered by DISPATCH on an event variant
	Event {
		sumtype_id: SumTypeId,
		variant_tag: u8,
	},
}

impl Default for ProcedureTrigger {
	fn default() -> Self {
		ProcedureTrigger::Call
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureDef {
	pub id: ProcedureId,
	pub namespace: NamespaceId,
	pub name: String,
	pub params: Vec<ProcedureParamDef>,
	pub return_type: Option<TypeConstraint>,
	/// RQL source text, compiled on load
	pub body: String,
	pub trigger: ProcedureTrigger,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureParamDef {
	pub name: String,
	pub param_type: TypeConstraint,
}
