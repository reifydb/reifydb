// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::interface::catalog::id::NamespaceId;

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceDef {
	pub id: NamespaceId,
	pub name: String,
}

impl NamespaceDef {
	pub fn system() -> Self {
		Self {
			id: NamespaceId(1),
			name: "system".to_string(),
		}
	}
}
