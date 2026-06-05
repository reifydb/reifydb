// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::interface::catalog::id::NamespaceId;

impl NamespaceId {
	pub const ROOT: NamespaceId = NamespaceId(0);
	pub const SYSTEM: NamespaceId = NamespaceId(1);
	pub const DEFAULT: NamespaceId = NamespaceId(2);
	pub const SYSTEM_CONFIG: NamespaceId = NamespaceId(3);
	pub const SYSTEM_METRICS: NamespaceId = NamespaceId(4);
	pub const SYSTEM_METRICS_STORAGE: NamespaceId = NamespaceId(5);
	pub const SYSTEM_METRICS_CDC: NamespaceId = NamespaceId(6);
	pub const SYSTEM_PROCEDURES: NamespaceId = NamespaceId(7);
	pub const SYSTEM_BINDINGS: NamespaceId = NamespaceId(8);
	pub const RQL: NamespaceId = NamespaceId(9);
	pub const SYSTEM_METRICS_PROFILER: NamespaceId = NamespaceId(10);
	pub const SYSTEM_METRICS_PROFILER_QUERY: NamespaceId = NamespaceId(11);
	pub const SYSTEM_METRICS_PROFILER_TXN: NamespaceId = NamespaceId(12);
	pub const SYSTEM_METRICS_PROFILER_STORAGE: NamespaceId = NamespaceId(13);
	pub const SYSTEM_METRICS_PROFILER_PLAN: NamespaceId = NamespaceId(14);
	pub const SYSTEM_METRICS_PROFILER_CDC: NamespaceId = NamespaceId(15);
	pub const SYSTEM_METRICS_PROFILER_FLOW: NamespaceId = NamespaceId(16);
	pub const SYSTEM_METRICS_RUNTIME: NamespaceId = NamespaceId(17);
	pub const SYSTEM_METRICS_RUNTIME_MEMORY: NamespaceId = NamespaceId(18);
	pub const SYSTEM_METRICS_RUNTIME_WATERMARKS: NamespaceId = NamespaceId(19);
	pub const SYSTEM_METRICS_PROFILER_SUBSCRIPTION: NamespaceId = NamespaceId(20);
	pub const SYSTEM_METRICS_PROFILER_SERVER: NamespaceId = NamespaceId(21);
	pub const SYSTEM_METRICS_PROFILER_WIRE: NamespaceId = NamespaceId(22);
	pub const SYSTEM_METRICS_PROFILER_AUTH: NamespaceId = NamespaceId(23);
	pub const SYSTEM_METRICS_PROFILER_CATALOG: NamespaceId = NamespaceId(24);
	pub const SYSTEM_METRICS_PROFILER_ENGINE: NamespaceId = NamespaceId(25);
	pub const SYSTEM_METRICS_PROFILER_MUTATE: NamespaceId = NamespaceId(26);
	pub const SYSTEM_METRICS_PROFILER_TRANSPORT: NamespaceId = NamespaceId(27);
	pub const SYSTEM_METRICS_PROFILER_TASK: NamespaceId = NamespaceId(28);
	pub const SYSTEM_METRICS_PROFILER_POLICY: NamespaceId = NamespaceId(29);
	pub const SYSTEM_METRICS_PROFILER_FFI: NamespaceId = NamespaceId(30);
	pub const SYSTEM_METRICS_PROFILER_CACHE: NamespaceId = NamespaceId(31);
	pub const SYSTEM_METRICS_PROFILER_SHAPE: NamespaceId = NamespaceId(32);
	pub const SYSTEM_METRICS_PROFILER_API: NamespaceId = NamespaceId(33);
	pub const SYSTEM_METRICS_PROFILER_ACTOR: NamespaceId = NamespaceId(34);
}

#[derive(Debug, Clone, PartialEq)]
pub enum Namespace {
	Local {
		id: NamespaceId,
		name: String,
		local_name: String,
		parent_id: NamespaceId,
	},
	Remote {
		id: NamespaceId,
		name: String,
		local_name: String,
		parent_id: NamespaceId,
		address: String,
		token: Option<String>,
	},
}

impl Namespace {
	pub fn id(&self) -> NamespaceId {
		match self {
			Namespace::Local {
				id,
				..
			}
			| Namespace::Remote {
				id,
				..
			} => *id,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			Namespace::Local {
				name,
				..
			}
			| Namespace::Remote {
				name,
				..
			} => name,
		}
	}

	pub fn local_name(&self) -> &str {
		match self {
			Namespace::Local {
				local_name,
				..
			}
			| Namespace::Remote {
				local_name,
				..
			} => local_name,
		}
	}

	pub fn parent_id(&self) -> NamespaceId {
		match self {
			Namespace::Local {
				parent_id,
				..
			}
			| Namespace::Remote {
				parent_id,
				..
			} => *parent_id,
		}
	}

	pub fn address(&self) -> Option<&str> {
		match self {
			Namespace::Remote {
				address,
				..
			} => Some(address),
			_ => None,
		}
	}

	pub fn token(&self) -> Option<&str> {
		match self {
			Namespace::Remote {
				token,
				..
			} => token.as_deref(),
			_ => None,
		}
	}

	pub fn is_remote(&self) -> bool {
		matches!(self, Namespace::Remote { .. })
	}

	pub fn system() -> Self {
		Self::Local {
			id: NamespaceId::SYSTEM,
			name: "system".to_string(),
			local_name: "system".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}

	pub fn default_namespace() -> Self {
		Self::Local {
			id: NamespaceId::DEFAULT,
			name: "default".to_string(),
			local_name: "default".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}
}
