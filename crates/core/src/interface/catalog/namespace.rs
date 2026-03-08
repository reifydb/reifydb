// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::interface::catalog::id::NamespaceId;

impl NamespaceId {
	/// Root sentinel — all top-level namespaces have `parent_id: NamespaceId::ROOT`.
	/// This is not a real namespace, just the tree root.
	pub const ROOT: NamespaceId = NamespaceId(0);
}

#[derive(Debug, Clone, PartialEq)]
pub enum Namespace {
	Local {
		id: NamespaceId,
		name: String,
		parent_id: NamespaceId,
	},
	Remote {
		id: NamespaceId,
		name: String,
		parent_id: NamespaceId,
		address: String,
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

	pub fn is_remote(&self) -> bool {
		matches!(self, Namespace::Remote { .. })
	}

	pub fn system() -> Self {
		Self::Local {
			id: NamespaceId(1),
			name: "system".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}

	pub fn default_namespace() -> Self {
		Self::Local {
			id: NamespaceId(2),
			name: "default".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}
}
