// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, ProcedureId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureDef {
	pub id: ProcedureId,
	pub namespace: NamespaceId,
	pub name: String,
	pub params: Vec<ProcedureParamDef>,
	pub return_type: Option<TypeConstraint>,
	/// RQL source text, compiled on load
	pub body: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureParamDef {
	pub name: String,
	pub param_type: TypeConstraint,
}
