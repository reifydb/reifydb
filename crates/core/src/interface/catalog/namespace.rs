// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::interface::catalog::id::NamespaceId;

impl NamespaceId {
	/// Root sentinel — all top-level namespaces have `parent_id: NamespaceId::ROOT`.
	/// This is not a real namespace, just the tree root.
	pub const ROOT: NamespaceId = NamespaceId(0);
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceDef {
	pub id: NamespaceId,
	pub name: String,
	pub parent_id: NamespaceId,
}

impl NamespaceDef {
	pub fn system() -> Self {
		Self {
			id: NamespaceId(1),
			name: "system".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}

	pub fn default_namespace() -> Self {
		Self {
			id: NamespaceId(2),
			name: "default".to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}
}
