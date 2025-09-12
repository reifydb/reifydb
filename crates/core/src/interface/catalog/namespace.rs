// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::NamespaceId;

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
