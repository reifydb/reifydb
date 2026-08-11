// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::PathBuf;

use reifydb_value::value::{constraint::TypeConstraint, sumtype::VariantRef};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{NamespaceId, ProcedureId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum RqlTrigger {
	#[default]
	Call,

	Event {
		variant: VariantRef,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ProcedureKind {
	Rql,
	Test,
	InProcess,
	ExternC,
	ExternWasm,
}

impl ProcedureKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			ProcedureKind::Rql => "rql",
			ProcedureKind::Test => "test",
			ProcedureKind::InProcess => "in_process",
			ProcedureKind::ExternC => "extern_c",
			ProcedureKind::ExternWasm => "extern_wasm",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ExternWasmModuleId(pub u64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureParam {
	pub name: String,
	pub param_type: TypeConstraint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Procedure {
	Rql {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		body: String,
		trigger: RqlTrigger,
	},

	Test {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		body: String,
	},

	InProcess {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		handler_name: String,
	},

	ExternC {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		handler_name: String,
		library_path: PathBuf,
		entry_symbol: String,
	},

	ExternWasm {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		handler_name: String,
		module_id: ExternWasmModuleId,
	},
}

impl Procedure {
	pub fn id(&self) -> ProcedureId {
		match self {
			Procedure::Rql {
				id,
				..
			}
			| Procedure::Test {
				id,
				..
			}
			| Procedure::InProcess {
				id,
				..
			}
			| Procedure::ExternC {
				id,
				..
			}
			| Procedure::ExternWasm {
				id,
				..
			} => *id,
		}
	}

	pub fn namespace(&self) -> NamespaceId {
		match self {
			Procedure::Rql {
				namespace,
				..
			}
			| Procedure::Test {
				namespace,
				..
			}
			| Procedure::InProcess {
				namespace,
				..
			}
			| Procedure::ExternC {
				namespace,
				..
			}
			| Procedure::ExternWasm {
				namespace,
				..
			} => *namespace,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			Procedure::Rql {
				name,
				..
			}
			| Procedure::Test {
				name,
				..
			}
			| Procedure::InProcess {
				name,
				..
			}
			| Procedure::ExternC {
				name,
				..
			}
			| Procedure::ExternWasm {
				name,
				..
			} => name.as_str(),
		}
	}

	pub fn params(&self) -> &[ProcedureParam] {
		match self {
			Procedure::Rql {
				params,
				..
			}
			| Procedure::Test {
				params,
				..
			}
			| Procedure::InProcess {
				params,
				..
			}
			| Procedure::ExternC {
				params,
				..
			}
			| Procedure::ExternWasm {
				params,
				..
			} => params,
		}
	}

	pub fn return_type(&self) -> Option<&TypeConstraint> {
		match self {
			Procedure::Rql {
				return_type,
				..
			}
			| Procedure::Test {
				return_type,
				..
			}
			| Procedure::InProcess {
				return_type,
				..
			}
			| Procedure::ExternC {
				return_type,
				..
			}
			| Procedure::ExternWasm {
				return_type,
				..
			} => return_type.as_ref(),
		}
	}

	pub fn kind(&self) -> ProcedureKind {
		match self {
			Procedure::Rql {
				..
			} => ProcedureKind::Rql,
			Procedure::Test {
				..
			} => ProcedureKind::Test,
			Procedure::InProcess {
				..
			} => ProcedureKind::InProcess,
			Procedure::ExternC {
				..
			} => ProcedureKind::ExternC,
			Procedure::ExternWasm {
				..
			} => ProcedureKind::ExternWasm,
		}
	}

	pub fn is_persistent(&self) -> bool {
		matches!(self, Procedure::Rql { .. } | Procedure::Test { .. })
	}

	pub fn event_variant(&self) -> Option<VariantRef> {
		match self {
			Procedure::Rql {
				trigger: RqlTrigger::Event {
					variant,
				},
				..
			} => Some(*variant),
			_ => None,
		}
	}

	pub fn handler_name(&self) -> Option<&str> {
		match self {
			Procedure::InProcess {
				handler_name,
				..
			}
			| Procedure::ExternC {
				handler_name,
				..
			}
			| Procedure::ExternWasm {
				handler_name,
				..
			} => Some(handler_name.as_str()),
			_ => None,
		}
	}

	pub fn body(&self) -> Option<&str> {
		match self {
			Procedure::Rql {
				body,
				..
			}
			| Procedure::Test {
				body,
				..
			} => Some(body.as_str()),
			_ => None,
		}
	}
}
