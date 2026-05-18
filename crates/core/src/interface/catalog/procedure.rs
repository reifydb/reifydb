// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::PathBuf;

use reifydb_type::value::{constraint::TypeConstraint, sumtype::VariantRef};
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
	Native,
	Ffi,
	Wasm,
}

impl ProcedureKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			ProcedureKind::Rql => "rql",
			ProcedureKind::Test => "test",
			ProcedureKind::Native => "native",
			ProcedureKind::Ffi => "ffi",
			ProcedureKind::Wasm => "wasm",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct WasmModuleId(pub u64);

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

	Native {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		native_name: String,
	},

	Ffi {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		native_name: String,
		library_path: PathBuf,
		entry_symbol: String,
	},

	Wasm {
		id: ProcedureId,
		namespace: NamespaceId,
		name: String,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		native_name: String,
		module_id: WasmModuleId,
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
			| Procedure::Native {
				id,
				..
			}
			| Procedure::Ffi {
				id,
				..
			}
			| Procedure::Wasm {
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
			| Procedure::Native {
				namespace,
				..
			}
			| Procedure::Ffi {
				namespace,
				..
			}
			| Procedure::Wasm {
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
			| Procedure::Native {
				name,
				..
			}
			| Procedure::Ffi {
				name,
				..
			}
			| Procedure::Wasm {
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
			| Procedure::Native {
				params,
				..
			}
			| Procedure::Ffi {
				params,
				..
			}
			| Procedure::Wasm {
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
			| Procedure::Native {
				return_type,
				..
			}
			| Procedure::Ffi {
				return_type,
				..
			}
			| Procedure::Wasm {
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
			Procedure::Native {
				..
			} => ProcedureKind::Native,
			Procedure::Ffi {
				..
			} => ProcedureKind::Ffi,
			Procedure::Wasm {
				..
			} => ProcedureKind::Wasm,
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

	pub fn native_name(&self) -> Option<&str> {
		match self {
			Procedure::Native {
				native_name,
				..
			}
			| Procedure::Ffi {
				native_name,
				..
			}
			| Procedure::Wasm {
				native_name,
				..
			} => Some(native_name.as_str()),
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
