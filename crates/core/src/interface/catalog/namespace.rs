// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::interface::catalog::id::{NamespaceId, RESERVED_USER_ID_START};

impl NamespaceId {
	pub const fn is_system(&self) -> bool {
		self.0 < RESERVED_USER_ID_START
	}

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
	pub const SYSTEM_METRICS_PROFILER_SPANS: NamespaceId = NamespaceId(11);
	pub const SYSTEM_METRICS_RUNTIME: NamespaceId = NamespaceId(12);
	pub const SYSTEM_METRICS_RUNTIME_MEMORY: NamespaceId = NamespaceId(13);
	pub const SYSTEM_METRICS_RUNTIME_WATERMARKS: NamespaceId = NamespaceId(14);
	pub const SYSTEM_METRICS_RUNTIME_OPERATORS: NamespaceId = NamespaceId(15);
	pub const SYSTEM_METRICS_PROC: NamespaceId = NamespaceId(16);
	pub const SYSTEM_METRICS_PROC_PROCESS: NamespaceId = NamespaceId(17);
	pub const SYSTEM_METRICS_PROC_PROCESS_IO: NamespaceId = NamespaceId(18);
	pub const SYSTEM_METRICS_PROC_PROCESS_MEMORY: NamespaceId = NamespaceId(19);
	pub const SYSTEM_METRICS_PROC_PROCESS_SCHED: NamespaceId = NamespaceId(20);
	pub const SYSTEM_METRICS_PROC_CGROUP: NamespaceId = NamespaceId(21);
	pub const SYSTEM_METRICS_PROC_CGROUP_IO: NamespaceId = NamespaceId(22);
	pub const SYSTEM_METRICS_PROC_CGROUP_MEMORY: NamespaceId = NamespaceId(23);
	pub const SYSTEM_METRICS_PROC_CGROUP_CPU: NamespaceId = NamespaceId(24);
	pub const SYSTEM_METRICS_PROC_CGROUP_PRESSURE: NamespaceId = NamespaceId(25);
	pub const SYSTEM_METRICS_STORE: NamespaceId = NamespaceId(26);
	pub const SYSTEM_METRICS_STORE_MULTI: NamespaceId = NamespaceId(27);
	pub const SYSTEM_METRICS_STORE_MULTI_COMMIT: NamespaceId = NamespaceId(28);
	pub const SYSTEM_METRICS_STORE_MULTI_PERSISTENT: NamespaceId = NamespaceId(29);
	pub const SYSTEM_METRICS_STORE_SINGLE: NamespaceId = NamespaceId(30);
	pub const SYSTEM_METRICS_STORE_SINGLE_COMMIT: NamespaceId = NamespaceId(31);
	pub const SYSTEM_METRICS_STORE_SINGLE_PERSISTENT: NamespaceId = NamespaceId(32);
	pub const SYSTEM_METRICS_STORE_OPERATOR: NamespaceId = NamespaceId(33);
	pub const SYSTEM_METRICS_STORE_OPERATOR_POINT: NamespaceId = NamespaceId(36);
	pub const SYSTEM_METRICS_STORE_OPERATOR_PERSISTENT: NamespaceId = NamespaceId(37);
	pub const SYSTEM_METRICS_INSTRUMENTS: NamespaceId = NamespaceId(38);
	pub const SYSTEM_METRICS_EPOCH: NamespaceId = NamespaceId(39);
	pub const SYSTEM_METRICS_LIFECYCLE: NamespaceId = NamespaceId(40);
	pub const SYSTEM_SOURCE: NamespaceId = NamespaceId(41);
	pub const STORAGE: NamespaceId = NamespaceId(42);
	pub const SYSTEM_METRICS_FLOW: NamespaceId = NamespaceId(43);
	pub const SYSTEM_METRICS_FLOW_STATE: NamespaceId = NamespaceId(44);
	pub const SYSTEM_METRICS_STORE_OPERATOR_POINT_KEYSPACE: NamespaceId = NamespaceId(45);
	pub const SYSTEM_METRICS_STORE_OPERATOR_RANGE: NamespaceId = NamespaceId(46);
	pub const SYSTEM_METRICS_STORE_OPERATOR_RANGE_KEYSPACE: NamespaceId = NamespaceId(47);
	pub const SYSTEM_METRICS_STORE_MULTI_RANGE: NamespaceId = NamespaceId(52);
	pub const SYSTEM_METRICS_STORE_MULTI_POINT: NamespaceId = NamespaceId(53);
	pub const SYSTEM_METRICS_STORE_CDC: NamespaceId = NamespaceId(48);
	pub const SYSTEM_METRICS_STORE_CDC_COMMIT: NamespaceId = NamespaceId(49);
	pub const SYSTEM_METRICS_STORE_CDC_READ: NamespaceId = NamespaceId(50);
	pub const SYSTEM_METRICS_STORE_CDC_PERSISTENT: NamespaceId = NamespaceId(51);
	pub const GRAPHQL: NamespaceId = NamespaceId(55);
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
