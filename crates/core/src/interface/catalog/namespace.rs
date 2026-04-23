// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::interface::catalog::id::NamespaceId;

impl NamespaceId {
	/// Root sentinel - all top-level namespaces have `parent_id: NamespaceId::ROOT`.
	/// This is not a real namespace, just the tree root.
	pub const ROOT: NamespaceId = NamespaceId(0);
	pub const SYSTEM: NamespaceId = NamespaceId(1);
	pub const DEFAULT: NamespaceId = NamespaceId(2);
	pub const SYSTEM_CONFIG: NamespaceId = NamespaceId(3);
	pub const SYSTEM_METRICS: NamespaceId = NamespaceId(4);
	pub const SYSTEM_METRICS_STORAGE: NamespaceId = NamespaceId(5);
	pub const SYSTEM_METRICS_CDC: NamespaceId = NamespaceId(6);
	pub const SYSTEM_PROCEDURES: NamespaceId = NamespaceId(7);
	pub const SYSTEM_BINDINGS: NamespaceId = NamespaceId(8);
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
